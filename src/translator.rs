//! Azure Translator (text API v3.0): translates each final transcript.
//!
//! Documented at <https://learn.microsoft.com/azure/ai-services/translator/text-translation/reference/v3/translate>:
//! `POST /translate?api-version=3.0&from=..&to=..`, the key in `Ocp-Apim-Subscription-Key`, the
//! resource's region in `Ocp-Apim-Subscription-Region`, a body `[{"Text": ".."}]`, an answer
//! `[{"translations": [{"text": "..", "to": ".."}]}]`.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::json;

/// Global endpoint. `https://api-eur.cognitive.microsofttranslator.com` keeps the processing in
/// the EU (set it with `AZURE_TRANSLATOR_URL`).
pub const DEFAULT_URL: &str = "https://api.cognitive.microsofttranslator.com";

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct TranslatorClient {
    http: reqwest::Client,
    base_url: String,
    key: String,
    region: Option<String>,
}

impl TranslatorClient {
    pub fn new(base_url: &str, key: &str, region: Option<&str>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_owned(),
            key: key.to_owned(),
            region: region.map(str::to_owned),
        }
    }

    /// Translates `text` from `from` to `to` (Translator language codes, see [`translator_code`]).
    ///
    /// # Errors
    ///
    /// Translator unreachable, refusing, or answering something unexpected. The error holds the
    /// status only: neither the text nor the answer, which would carry what someone said.
    pub async fn translate(&self, text: &str, from: &str, to: &str) -> Result<String> {
        let mut url = reqwest::Url::parse(&format!("{}/translate", self.base_url))
            .context("invalid AZURE_TRANSLATOR_URL")?;
        url.query_pairs_mut()
            .append_pair("api-version", "3.0")
            .append_pair("from", from)
            .append_pair("to", to);
        let mut request = self
            .http
            .post(url)
            .header("Ocp-Apim-Subscription-Key", &self.key)
            .json(&json!([{ "Text": text }]))
            .timeout(TIMEOUT);
        if let Some(region) = &self.region {
            request = request.header("Ocp-Apim-Subscription-Region", region);
        }
        let response = request
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("Azure Translator unreachable ({})", kind(&err)))?;
        let status = response.status();
        if !status.is_success() {
            bail!("Azure Translator answered {status}");
        }
        #[derive(Deserialize)]
        struct Item {
            translations: Vec<Translation>,
        }
        #[derive(Deserialize)]
        struct Translation {
            text: String,
        }
        let items: Vec<Item> = response
            .json()
            .await
            .map_err(|_| anyhow::anyhow!("unexpected Azure Translator answer"))?;
        items
            .into_iter()
            .next()
            .and_then(|item| item.translations.into_iter().next())
            .map(|t| t.text.trim().to_owned())
            .context("empty Azure Translator answer")
    }
}

fn kind(err: &reqwest::Error) -> &'static str {
    if err.is_timeout() {
        "timeout"
    } else if err.is_connect() {
        "connection refused or failed"
    } else {
        "request failed"
    }
}

/// Translator language code of a locale or language tag: `fr-FR` gives `fr`, `zh-CN` gives
/// `zh-Hans`, `sr-Latn-RS` gives `sr-Latn`; `pt-PT` and `fr-CA` keep their own codes.
pub fn translator_code(tag: &str) -> String {
    let lower = tag.trim().to_ascii_lowercase();
    match lower.as_str() {
        "zh-cn" | "zh-sg" | "zh-hans" | "zh-hans-cn" => return "zh-Hans".to_owned(),
        "zh-tw" | "zh-hk" | "zh-mo" | "zh-hant" | "zh-hant-tw" => return "zh-Hant".to_owned(),
        "pt-pt" | "fr-ca" => return lower,
        _ => {}
    }
    let mut parts = lower.split('-');
    let language = parts.next().unwrap_or_default().to_owned();
    match parts.next() {
        Some(script) if script.len() == 4 => {
            let mut chars = script.chars();
            let mut out = language;
            out.push('-');
            out.extend(chars.next().map(|c| c.to_ascii_uppercase()));
            out.extend(chars);
            out
        }
        _ => language,
    }
}

/// Parses `livecap.target`: one Translator code (`fr`, `zh-Hans`) or locale (`fr-FR`).
///
/// Returns the Translator code, or `None` for an empty or malformed value.
pub fn parse_target(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let mut parts = raw.split('-');
    let language = parts.next()?;
    let subtags: Vec<&str> = parts.collect();
    let valid = (2..=3).contains(&language.len())
        && language.bytes().all(|b| b.is_ascii_alphabetic())
        && subtags.len() <= 2
        && subtags
            .iter()
            .all(|s| (2..=4).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric()));
    valid.then(|| translator_code(raw))
}

/// True when text in locale `detected` needs no translation for a reader of `target`.
pub fn same_language(detected: &str, target: &str) -> bool {
    translator_code(detected).eq_ignore_ascii_case(&translator_code(target))
}

#[cfg(test)]
mod tests {
    use super::{parse_target, same_language, translator_code};

    #[test]
    fn locales_map_to_translator_codes() {
        assert_eq!(translator_code("fr-FR"), "fr");
        assert_eq!(translator_code("en-US"), "en");
        assert_eq!(translator_code("zh-CN"), "zh-Hans");
        assert_eq!(translator_code("zh-TW"), "zh-Hant");
        assert_eq!(translator_code("sr-Latn-RS"), "sr-Latn");
        assert_eq!(translator_code("pt-PT"), "pt-pt");
        assert_eq!(translator_code("pt-BR"), "pt");
        assert_eq!(translator_code("fil-PH"), "fil");
        assert_eq!(translator_code("fr"), "fr");
    }

    #[test]
    fn targets_are_codes_or_locales() {
        assert_eq!(parse_target("fr").as_deref(), Some("fr"));
        assert_eq!(parse_target(" EN-us ").as_deref(), Some("en"));
        assert_eq!(parse_target("zh-Hans").as_deref(), Some("zh-Hans"));
        for raw in ["", " ", "f", "fren", "fr,en", "fr_FR", "fr-", "12"] {
            assert_eq!(parse_target(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn a_reader_of_the_spoken_language_needs_no_translation() {
        assert!(same_language("fr-FR", "fr"));
        assert!(same_language("fr-CA", "fr-ca"));
        assert!(!same_language("fr-FR", "en"));
        assert!(!same_language("zh-CN", "zh-Hant"));
    }
}
