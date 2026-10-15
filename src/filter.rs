//! Speech-to-text models of the Whisper family invent short phrases on silence or noise
//! ("Thank you.", "Merci.", "T-Series"…). When a whole utterance is one of them, it is
//! flagged so it can be kept out of the captions shown to the other person.

const EXACT: &[&str] = &[
    "merci",
    "merci.",
    "thank you",
    "thank you.",
    "thanks for watching",
    "thanks for watching!",
    "t-series",
    "shabbat shalom",
];

const CONTAINS: &[&str] = &["amara.org", "sous-titrage", "sous-titres réalisés"];

/// True when the utterance, taken as a whole, looks like a hallucination on silence.
pub fn is_suspect(text: &str) -> bool {
    let t = text.trim().to_lowercase();
    if t.is_empty() || t.chars().all(|c| c == '.' || c == '…') {
        return true;
    }
    EXACT.contains(&t.as_str()) || CONTAINS.iter().any(|needle| t.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::is_suspect;

    #[test]
    fn flags_whole_utterance_hallucinations() {
        assert!(is_suspect("Merci."));
        assert!(is_suspect(" Thank you. "));
        assert!(is_suspect("T-Series"));
        assert!(is_suspect("..."));
        assert!(is_suspect("Sous-titrage ST' 501"));
    }

    #[test]
    fn keeps_real_sentences() {
        assert!(!is_suspect("Merci beaucoup Hery"));
        assert!(!is_suspect(
            "Je vais aller chercher un petit peu de baguette."
        ));
        assert!(!is_suspect("Misaotra"));
    }
}
