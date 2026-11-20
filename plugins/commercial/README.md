# Optional commercial source helper

This directory is intentionally not part of the public repository. If you have
the private helper from the original deployment, place it here as
`plugins/commercial/helper.py` together with its private `app` runtime package.

The Go station defaults to `RADIO_PRIVATE_HELPER=plugins/commercial/helper.py`.
Without it, local library playback and the core station still work; NetEase,
QQ and Bilibili source resolution/lyrics are unavailable.
