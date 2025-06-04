# NEVER create any mod.rs file

The source code for module *mymodule* goes into `mymodule.rs` *NOT*
`mymodule/mod.rs`.

DO NOT, UNDER ANY CIRCUMSTANCES, CREATE A FILE CALLED `mod.rs`

## Examples

src/media/mod.rs // BAD
src/media/processor/mod.rs // BAD
src/commands/mod.rs // BAD

src/media.rs // CORRECT
src/media/processor.rs // CORRECT
src/commands.rs // CORRECT
