param(
  [int]$Count = 10,
  [int]$StartPort = 1201
)

for ($i = 1; $i -le $Count; $i++) {
  $port = $StartPort + $i - 1
  @"
  rsshub$i:
    image: diygod/rsshub:latest
    ports:
      - "127.0.0.1:$port`:1200"
    environment:
      CACHE_EXPIRE: 30
      TWITTER_AUTH_TOKEN: "`$`{RSSHUB${i}_TWITTER_AUTH_TOKEN:-`}"
      TWITTER_THIRD_PARTY_API: "`$`{RSSHUB${i}_TWITTER_THIRD_PARTY_API:-`}"
      PROXY_URI: "`$`{RSSHUB${i}_PROXY_URI:-`}"
    restart: unless-stopped

"@
}
