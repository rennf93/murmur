# Security

- Unix domain socket only, mode 0600; no TCP. The home is 0700; the token file, the database and its WAL files are 0600.
- The first line on any connection must be `auth` with the token. That is the whole trust boundary: same OS user, by design.
- Messages are context a session reads, never permission decisions. Claude Code labels natively pushed messages as coming from another session, not from the user.
- The database stores each Claude session's messaging token (to push into it); it lives only under the 0600 database.
- A message is a claim, not evidence. Sessions are told to verify against a message's `references` before agreeing.
- `murmur install` only merges into config files, backs each up once as `.bak-murmur`, and refuses to touch a file that is not valid JSON.

To report a vulnerability, see [SECURITY.md](https://github.com/rennf93/murmur/blob/master/SECURITY.md).
