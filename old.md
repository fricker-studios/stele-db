Release Pipeline:
    - create release tag & push latest tag
    - publish release (with notes)
    - build & publish artifacts
    - build & publish distributed images
    - build & publish docs page
    - build & publish operator
    - build & publish helixAdmin

Products:
    - Public-facing repo(s)
        - not any core IP code (none of the helix-* crates)
        - referenced in the marketing UI
        - probably used to ship the release code binaries?
    - Marketing site (helixdb.us/helixdb.app) - separate repo?
        - auto-maintained docs site (by version/release)
        - all-in-one docs (cli, db, helix-admin, etc)
        - releases
        - roadmap/feature suggestions/issues(github?)
        - eventually: helix cloud (cloud provider for db instances)
    - public binaries for db engine install/CLI (and installation instructions)
    - public docker image(s) for docker-hosted db instances
        - ships lightweight db container with running helixdb on port
    - K8s/Openshift operator - separate repo?
    - binaries/installer for helix admin GUI - separate repo
    - DB flavor on cloud providers (eventually, and limited support - single node/etc)
