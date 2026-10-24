These DER files are a self-signed certificate for `127.0.0.1` and its test-only
PKCS#8 private key. The TLS policy tests present this certificate to exercise
certificate rejection. It is deliberately absent from the client's trust store.
No certificate or key is used by production code or by the PostgreSQL fixture.

To replace the pair, generate a self-signed RSA certificate with an IP subject
alternative name of `127.0.0.1`, export the certificate as DER, and export the key
as unencrypted PKCS#8 DER. Commit both files together.
