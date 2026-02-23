ROOT + Jupyter Setup

1)  Install ROOT and tbb: brew install root tbb

2)  Start Jupyter with ROOT: 
    `source “$(brew –prefixroot)/bin/thisroot.sh” `
    `uv run jupyter lab`

3)  Bookkeeping PEM location: under `../permissions/` folder. Instructions for .pem generation in  `docs/`