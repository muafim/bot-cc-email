use serde::{Deserialize, Serialize};

use crate::agent::command_runner::{AgentResult, AgentRunner};
use crate::attachment::GeneratedFile;
use crate::error::{CcEmailError, Result};

// ---------------------------------------------------------------------------
// Usage / event types (API-agnostic, kept compatible with the rest of the app)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UsageReport {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: Option<f64>, // Gemini free tier = $0, kept for compat
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub event_type: AgentEventType,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AgentEventType {
    Text(String),
    Thinking(String),
    ToolUse {
        name: String,
        input: String,
    },
    ToolResult {
        name: String,
        output: String,
        success: bool,
    },
    Error(String),
    Done {
        input_tokens: u64,
        output_tokens: u64,
    },
}

// ---------------------------------------------------------------------------
// Gemini REST response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsage>,
    error: Option<GeminiError>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    content: Option<GeminiContent>,
}

#[derive(Debug, Deserialize)]
struct GeminiContent {
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Deserialize)]
struct GeminiPart {
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiUsage {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: Option<u64>,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct GeminiError {
    message: String,
}

/// Parse "Please retry in 54.84s" from Gemini 429 responses.
fn parse_retry_secs(message: &str) -> u64 {
    message
        .find("retry in ")
        .and_then(|start| {
            let rest = &message[start + 9..];
            let num: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect();
            num.parse::<f64>().ok()
        })
        .map(|s| s.ceil() as u64)
        .unwrap_or(60)
        .clamp(1, 120)
}

fn format_gemini_error(status: reqwest::StatusCode, message: &str) -> CcEmailError {
    let mut msg = format!("Gemini API error (HTTP {}): {}", status, message);
    if message.contains("limit: 0") {
        msg.push_str(
            "\n\nKuota free tier untuk model ini tidak aktif (limit: 0). \
             Coba model gemini-2.5-flash-lite di config, buat API key baru di \
             https://aistudio.google.com/apikey, atau aktifkan billing (kuota gratis tetap berlaku): \
             https://ai.google.dev/gemini-api/docs/billing",
        );
    }
    CcEmailError::Agent(msg)
}

// ---------------------------------------------------------------------------
// Agent
// ---------------------------------------------------------------------------

pub struct GeminiAgent {
    api_key: String,
    model: String,
    timeout_seconds: u64,
    last_usage: std::sync::Mutex<UsageReport>,
}

impl GeminiAgent {
    /// `model` defaults to `"gemini-2.5-flash-lite"` if `None`.
    pub fn new(api_key: String, model: Option<String>, timeout_seconds: u64) -> Self {
        Self {
            api_key,
            model: model.unwrap_or_else(|| "gemini-2.5-flash-lite".to_string()),
            timeout_seconds,
            last_usage: std::sync::Mutex::new(UsageReport::default()),
        }
    }

    pub fn get_model(&self) -> &str {
        &self.model
    }

    pub fn set_model(&mut self, model: &str) {
        self.model = model.to_string();
    }

    pub fn last_usage(&self) -> UsageReport {
        self.last_usage.lock().unwrap().clone()
    }

    /// Core call: POST to Gemini generateContent REST endpoint (with 429 retry).
    async fn call_api(&self, prompt: &str) -> Result<(String, Vec<AgentEvent>)> {
        const MAX_ATTEMPTS: u32 = 3;

        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            self.model, self.api_key
        );

        let body = serde_json::json!({
            "contents": [
                {
                    "role": "user",
                    "parts": [{ "text": prompt }]
                }
            ],
            "generationConfig": {
                "temperature": 0.7,
                "maxOutputTokens": 8192
            }
        });

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(self.timeout_seconds))
            .build()
            .map_err(|e| CcEmailError::Agent(format!("failed to build HTTP client: {}", e)))?;

        for attempt in 1..=MAX_ATTEMPTS {
            let http_resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    if e.is_timeout() {
                        CcEmailError::Agent(format!(
                            "agent timed out after {}s",
                            self.timeout_seconds
                        ))
                    } else {
                        CcEmailError::Agent(format!("HTTP request failed: {}", e))
                    }
                })?;

            let status = http_resp.status();
            let resp: GeminiResponse = http_resp.json().await.map_err(|e| {
                CcEmailError::Agent(format!("failed to parse Gemini response: {}", e))
            })?;

            if let Some(err) = resp.error {
                let retryable = status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    && err.message.contains("retry in")
                    && attempt < MAX_ATTEMPTS;

                if retryable {
                    let wait = parse_retry_secs(&err.message);
                    tracing::warn!(
                        attempt,
                        wait_secs = wait,
                        model = %self.model,
                        "Gemini rate limited, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                    continue;
                }

                return Err(format_gemini_error(status, &err.message));
            }

            return self.parse_success_response(resp);
        }

        unreachable!()
    }

    fn parse_success_response(&self, resp: GeminiResponse) -> Result<(String, Vec<AgentEvent>)> {
        let result_text = resp
            .candidates
            .as_deref()
            .and_then(|c| c.first())
            .and_then(|c| c.content.as_ref())
            .map(|content| {
                content
                    .parts
                    .iter()
                    .filter_map(|p| p.text.as_deref())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();

        let mut events = Vec::new();
        if let Some(usage) = resp.usage_metadata {
            let input_tokens = usage.prompt_token_count.unwrap_or(0);
            let output_tokens = usage.candidates_token_count.unwrap_or(0);

            {
                let mut u = self.last_usage.lock().unwrap();
                u.input_tokens += input_tokens;
                u.output_tokens += output_tokens;
            }

            events.push(AgentEvent {
                event_type: AgentEventType::Done {
                    input_tokens,
                    output_tokens,
                },
                timestamp: chrono::Utc::now(),
            });
        }

        if !result_text.is_empty() {
            events.push(AgentEvent {
                event_type: AgentEventType::Text(result_text.clone()),
                timestamp: chrono::Utc::now(),
            });
        }

        Ok((result_text, events))
    }
}

// ---------------------------------------------------------------------------
// AgentRunner impl
// ---------------------------------------------------------------------------

impl AgentRunner for GeminiAgent {
    fn run(
        &self,
        prompt: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<AgentResult>> + Send + '_>> {
        let prompt = prompt.to_string();
        Box::pin(async move {
            tracing::info!(model = %self.model, "running gemini agent");

            let (text, _events) = self.call_api(&prompt).await?;

            Ok(AgentResult {
                success: true,
                stdout: text,
                stderr: String::new(),
                exit_code: Some(0),
                generated_files: Vec::<GeneratedFile>::new(),
            })
        })
    }
}

impl AgentRunner for std::sync::Arc<GeminiAgent> {
    fn run(
        &self,
        prompt: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<AgentResult>> + Send + '_>> {
        (**self).run(prompt)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_model() {
        let agent = GeminiAgent::new("fake_key".to_string(), None, 30);
        assert_eq!(agent.get_model(), "gemini-2.5-flash-lite");
    }

    #[test]
    fn test_custom_model() {
        let agent = GeminiAgent::new(
            "fake_key".to_string(),
            Some("gemini-2.5-flash".to_string()),
            30,
        );
        assert_eq!(agent.get_model(), "gemini-2.5-flash");
    }

    #[test]
    fn test_set_model() {
        let mut agent = GeminiAgent::new("fake_key".to_string(), None, 30);
        agent.set_model("gemini-2.0-flash-exp");
        assert_eq!(agent.get_model(), "gemini-2.0-flash-exp");
    }

    #[test]
    fn test_usage_initial_zero() {
        let agent = GeminiAgent::new("fake_key".to_string(), None, 30);
        let u = agent.last_usage();
        assert_eq!(u.input_tokens, 0);
        assert_eq!(u.output_tokens, 0);
    }

    #[test]
    fn test_parse_retry_secs() {
        let msg = "Please retry in 54.844968426s.";
        assert_eq!(parse_retry_secs(msg), 55);
    }
}
