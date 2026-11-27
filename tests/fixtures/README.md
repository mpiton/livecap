# Test fixtures

- `canary-fr.wav`: the French sentence "Bonjour, je m'appelle Claire et j'habite à Paris.", 3.7 s, PCM 16 kHz mono 16-bit, used by the canary tests against the real Azure AI Speech (`tests/canary.rs`). It is synthetic speech (the `fr-FR-DeniseNeural` voice through `edge-tts`), converted with `ffmpeg -ac 1 -ar 16000 -sample_fmt s16 -map_metadata -1 -bitexact`. Replace it with a human recording if you have one: the test only needs a French sentence containing "Paris".
