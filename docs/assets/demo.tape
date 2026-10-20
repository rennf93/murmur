# CLI demo with simulated peers. Run from the repository root after `npm run build`:
#   vhs docs/assets/demo.tape
# It uses an isolated home (MURMUR_HOME below), never ~/.murmur. The broker it
# autostarts stays running afterwards: stop it and delete the directory.
Output docs/assets/demo.gif

Set Shell bash
Set FontSize 15
Set Width 1000
Set Height 460
Set Padding 16
Set TypingSpeed 28ms
Set Theme "Dracula"

Hide
Type "export MURMUR_HOME=$TMPDIR/murmur-demo PS1='$ '" Enter
Type "murmur() { node bin/murmur.js ""$@""; }" Enter
Type "rm -rf $MURMUR_HOME; murmur init >/dev/null; murmur reply 0 x --as alice 2>/dev/null; murmur reply 0 x --as bob 2>/dev/null" Enter
Type "clear" Enter
Sleep 1s
Show

Type "murmur groups create review" Enter
Sleep 700ms
Type "murmur groups add review alice bob" Enter
Sleep 700ms
Type "murmur send review 'Who owns the migration?'" Enter
Sleep 1s
Type "murmur reply 1 'INFO I do, schema is in db/001.sql' --as alice --verdict info" Enter
Sleep 700ms
Type "murmur reply 1 'AGREE alice owns it' --as bob --verdict agree" Enter
Sleep 1s
Type "murmur thread 1" Enter
Sleep 2500ms
Type "murmur status" Enter
Sleep 2500ms
