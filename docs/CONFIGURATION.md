# Configuration

## Station

```text
RADIO_GO_ADDR                  default 127.0.0.1:8091
RADIO_GO_ADMIN_TOKEN          required for remote admin
RADIO_LIBRARY_DIRS            colon-separated local music roots
RADIO_GO_STATE                data/station-go-state.json
RADIO_GO_LIBRARY_CONFIG       data/station-go-library.json
RADIO_GO_UPLOADS              data/uploads
RADIO_GO_BACKGROUNDS          data/backgrounds
RADIO_GO_IDENTITY             data/station-go-identity.json
RADIO_STATIC_DIR              web/static
RADIO_PRIVATE_PYTHON          python3
RADIO_PRIVATE_HELPER          plugins/commercial/helper.py
RADIO_JOIN_REQUIRED           set 1 to require invite cookie
RADIO_JOIN_SECRET             invite cookie signing secret
RADIO_DIRECTORY_URL           Directory base URL
RADIO_DIRECTORY_REGISTER_TOKEN
RADIO_DIRECTORY_PUBLIC_URL    DIRECT station URL
RADIO_MESH_STUN               STUN URL
RADIO_HTTP_LEAF_MAX           max non-WebRTC leaves
```

## Relay

```text
RELAY_TOKEN                   shared station relay secret
RELAY_TLS_ADDR                default :9443
RELAY_HTTP_ADDR               default 127.0.0.1:18081
RELAY_CERT / RELAY_KEY        TLS certificate
RELAY_ALLOW_MEDIA             default 0; keep 0 for rendezvous-only
```

## Directory

```text
RADIO_DIRECTORY_ADDR          default 127.0.0.1:8787
RADIO_DIRECTORY_DATA          /var/lib/radio-directory/stations.json
RADIO_DIRECTORY_WEB           web/directory
RADIO_DIRECTORY_PUBLIC_URL    public join URL
RADIO_DIRECTORY_REGISTER_TOKEN
```
