# Diagrams

Three diagrams, each answering a different question. All are embedded in the
[project README](../../README.md#how-it-works) and downloadable here.

| File | Question it answers | Formats |
|---|---|---|
| `aws-architecture` | Where does everything run, and what talks to what? | [SVG](aws-architecture.svg) · [PNG](aws-architecture.png) |
| `migration-sequence` | In what order does it happen, and what runs concurrently? | [SVG](migration-sequence.svg) · [PNG](migration-sequence.png) · [.mmd](migration-sequence.mmd) |
| `event-lifecycle-flow` | What are all the paths one row can take, including the failures? | [SVG](event-lifecycle-flow.svg) · [PNG](event-lifecycle-flow.png) · [.mmd](event-lifecycle-flow.mmd) |

## Regenerating

The architecture diagram is generated from a script rather than hand-drawn, so
that editing it produces a reviewable diff instead of a redraw and the geometry
cannot drift:

```bash
python3 generate_aws_architecture.py > aws-architecture.svg
```

The other two are Mermaid. GitHub renders Mermaid natively in the README, so the
fenced blocks there are the canonical copies and the `.mmd` files are the source
you edit. Export raster and vector versions with:

```bash
npx -y @mermaid-js/mermaid-cli -i migration-sequence.mmd   -o migration-sequence.png   -b white -w 2400
npx -y @mermaid-js/mermaid-cli -i event-lifecycle-flow.mmd -o event-lifecycle-flow.png -b white -w 2600
```

## Conventions

Colour carries meaning consistently across all three, so a reader who learns it
once does not have to relearn it:

| Colour | Meaning |
|---|---|
| Green | The bulk part path — extract, object storage, native import, fenced merge |
| Purple | The change stream — source log, connector, broker, fenced apply |
| Blue | Target writes and reads |
| Red | Key material and failure handling |
| Amber | The two decisions that gate everything: the LSN fence and the cutover gate |

The palette is distinguishable in greyscale, because architecture diagrams get
printed for design reviews.

## Two things worth knowing if you edit these

**The SVG uses presentation attributes, not a `<style>` block.** GitHub serves
README images through a proxy that has at various times stripped embedded CSS. A
diagram that renders in a browser but collapses in the README is worse than no
diagram.

**A bare `%%` line breaks Mermaid's flowchart parser.** It is a valid comment to
the sequence parser and a syntax error to the flowchart parser, which fails with
a misleading "Parse error on line 1". Use `%% ---` for a spacer comment.
