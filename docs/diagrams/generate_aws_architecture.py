#!/usr/bin/env python3
"""Generate the AWS deployment diagram as a self-contained SVG.

The diagram is generated rather than hand-drawn so that the geometry stays
consistent when it is edited, and so that a change to the architecture is a
reviewable diff rather than a redraw.

Presentation attributes are written directly onto every element instead of using
a <style> block: GitHub serves README images through a proxy that has, at various
times, stripped embedded CSS, and a diagram that renders correctly in a browser
but collapses in the README is worse than no diagram.

Usage:
    python3 generate_aws_architecture.py > aws-architecture.svg
"""

import sys

# ----------------------------------------------------------------- palette
#
# Chosen for legibility against a light card in both GitHub themes, and to stay
# distinguishable in greyscale when someone prints the architecture for a review.

INK = "#16212E"          # primary text
MUTED = "#5C6F82"        # secondary text
HAIRLINE = "#C9D4DF"     # subtle borders
CARD = "#FFFFFF"

ONPREM_FILL = "#EDF2F7"
ONPREM_EDGE = "#8497AB"
AWS_FILL = "#FFF8F0"
AWS_EDGE = "#E08B2E"

STORAGE = "#2E7D57"      # object storage and the bulk part path
STREAM = "#6A4C93"       # change stream
DATA = "#1F5FA9"         # database writes
SECURITY = "#B4472A"     # key material
COMPUTE = "#35495E"      # services

SANS = "system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"
MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono',monospace"

W, H = 1480, 980
out = []


def add(s):
    out.append(s)


def esc(t):
    return (t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def rect(x, y, w, h, fill=CARD, stroke=HAIRLINE, rx=10, sw=1.25, dash=None, op=1.0):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" '
        f'fill="{fill}" fill-opacity="{op}" stroke="{stroke}" stroke-width="{sw}"{d}/>')


def text(x, y, s, size=12, fill=INK, weight="400", anchor="start",
         family=SANS, spacing=None, opacity=1.0):
    ls = f' letter-spacing="{spacing}"' if spacing else ""
    add(f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}" fill-opacity="{opacity}" '
        f'text-anchor="{anchor}"{ls}>{esc(s)}</text>')


def accent_bar(x, y, h, color):
    """A short vertical rule that colour-codes a card by role."""
    add(f'<rect x="{x}" y="{y}" width="4" height="{h}" rx="2" ry="2" fill="{color}"/>')


def card(x, y, w, h, title, sub=None, sub2=None, color=COMPUTE, mono=False,
         title_size=13, fill=CARD):
    rect(x, y, w, h, fill=fill)
    accent_bar(x + 10, y + 12, h - 24, color)
    tx = x + 24
    fam = MONO if mono else SANS
    if sub2:
        text(tx, y + h / 2 - 10, title, size=title_size, weight="600", family=fam)
        text(tx, y + h / 2 + 8, sub, size=10.5, fill=MUTED)
        text(tx, y + h / 2 + 23, sub2, size=10.5, fill=MUTED)
    elif sub:
        text(tx, y + h / 2 - 3, title, size=title_size, weight="600", family=fam)
        text(tx, y + h / 2 + 15, sub, size=10.5, fill=MUTED)
    else:
        text(tx, y + h / 2 + 5, title, size=title_size, weight="600", family=fam)


def poly(points, color, width=2.0, dash=None, marker=True):
    pts = " ".join(f"{px},{py}" for px, py in points)
    d = f' stroke-dasharray="{dash}"' if dash else ""
    m = f' marker-end="url(#a-{color[1:]})"' if marker else ""
    add(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{width}" '
        f'stroke-linejoin="round" stroke-linecap="round"{d}{m}/>')


def label(x, y, s, color, anchor="start", size=10):
    """A flow label with a halo so it stays readable where it crosses a line."""
    add(f'<text x="{x}" y="{y}" font-family="{SANS}" font-size="{size}" font-weight="600" '
        f'text-anchor="{anchor}" stroke="#FBFCFD" stroke-width="3.5" stroke-linejoin="round" '
        f'paint-order="stroke" fill="{color}">{esc(s)}</text>')


# ------------------------------------------------------------------ document

add(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
    f'viewBox="0 0 {W} {H}" role="img" '
    f'aria-label="AWS deployment architecture for the database migration platform">')
add('<title>db-migration-platform — AWS deployment architecture</title>')
add('<desc>Source network on the left holding the source database, the .dat extractor, '
    'the CDC connector and the tokenisation boundary backed by an HSM. A private AWS VPC '
    'on the right holding the S3 parts bucket, Amazon MSK, KMS, five ECS Fargate services, '
    'an Aurora cluster and observability. The two are joined by AWS Direct Connect.</desc>')

add('<defs>')
for c in (STORAGE, STREAM, DATA, SECURITY, COMPUTE, MUTED):
    add(f'<marker id="a-{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
        f'markerHeight="7" orient="auto-start-reverse">'
        f'<path d="M 0 1 L 10 5 L 0 9 z" fill="{c}"/></marker>')
add('</defs>')

add(f'<rect width="{W}" height="{H}" fill="#FBFCFD"/>')

# ------------------------------------------------------------------- header

text(40, 42, "db-migration-platform — AWS deployment", size=21, weight="700")
text(40, 66, "Private VPC, no internet gateway. Sensitive columns are protected before "
             "they leave the source network, and the target never holds plaintext for them.",
     size=12.5, fill=MUTED)

# --------------------------------------------------------------- on-premise

OX, OY, OW, OH = 32, 96, 372, 744
rect(OX, OY, OW, OH, fill=ONPREM_FILL, stroke=ONPREM_EDGE, rx=14, sw=1.5)
text(OX + 20, OY + 28, "SOURCE NETWORK  ·  ON-PREMISE", size=11, weight="700",
     fill="#43566B", spacing="1.1")

cx, cw = OX + 20, OW - 40

card(cx, 148, cw, 66, "Source database", "IBM DB2 · PostgreSQL · MySQL — still serving live traffic",
     color=DATA)

card(cx, 250, 158, 78, ".dat extractor", "size-rolled parts,", "sealed with SHA-256", color=STORAGE)
card(cx + 174, 250, 158, 78, "CDC connector", "Debezium · Qlik ·", "AWS DMS", color=STREAM)

card(cx, 364, cw, 74, "Protect  —  tokenise / encrypt",
     "the confidentiality boundary: nothing downstream of this line",
     "holds plaintext for a protected column", color=SECURITY)

card(cx, 470, cw, 62, "SafeNet HSM  ·  PKCS#11  ·  KMS",
     "unwraps one data key per process, never one call per row", color=SECURITY)

# Two annotations that carry the parts of the design a box cannot.
rect(cx, 560, cw, 82, fill="#F7FAFC", stroke=HAIRLINE, rx=8)
text(cx + 16, 582, "Watermarks", size=11, weight="700", fill="#43566B")
text(cx + 16, 600, "The extractor writes low and high watermarks to a signal", size=10.5, fill=MUTED)
text(cx + 16, 616, "table on the source, so they travel through the same", size=10.5, fill=MUTED)
text(cx + 16, 632, "transaction log and are ordered against the data changes.", size=10.5, fill=MUTED)

rect(cx, 658, cw, 66, fill="#F7FAFC", stroke=HAIRLINE, rx=8)
text(cx + 16, 680, "Verification reads back", size=11, weight="700", fill="#43566B")
text(cx + 16, 698, "The reconciler queries this database over the private", size=10.5, fill=MUTED)
text(cx + 16, 714, "circuit to compare digests against the target.", size=10.5, fill=MUTED)

# arrows inside the source network
poly([(cx + 79, 214), (cx + 79, 250)], STORAGE)
poly([(cx + 253, 214), (cx + 253, 250)], STREAM)
label(cx + 268, 238, "log", STREAM)
poly([(cx + 79, 328), (cx + 79, 364)], STORAGE)
poly([(cx + 253, 328), (cx + 253, 364)], STREAM)
poly([(cx + 166, 470), (cx + 166, 442)], SECURITY)

# ----------------------------------------------------------- direct connect

rect(428, 516, 60, 210, fill="#F7FAFC", stroke=ONPREM_EDGE, rx=10, dash="5 4")
add(f'<text transform="translate(464,621) rotate(-90)" font-family="{SANS}" font-size="11.5" '
    f'font-weight="700" fill="#43566B" text-anchor="middle" letter-spacing="0.8">'
    f'AWS Direct Connect</text>')

# --------------------------------------------------------------------- AWS

AX, AY, AW, AH = 512, 96, 936, 744
rect(AX, AY, AW, AH, fill=AWS_FILL, stroke=AWS_EDGE, rx=14, sw=1.5)
text(AX + 24, AY + 26, "AWS  ·  VPC — PRIVATE SUBNETS ACROSS 3 AZ  ·  NO INTERNET GATEWAY, NO NAT",
     size=11, weight="700", fill="#9A5B12", spacing="0.9")

IX, IW = 560, 864          # inner content
COLW = (IW - 48) / 3       # three columns of 272
C1, C2, C3 = IX, IX + COLW + 24, IX + 2 * (COLW + 24)

# Row A — ingress and key material
card(C1, 156, COLW, 80, "Amazon S3", "parts bucket · SSE-KMS", "reached by gateway endpoint", color=STORAGE)
card(C2, 156, COLW, 80, "Amazon MSK", "change stream · TLS + IAM", "retention outlasts the migration", color=STREAM)
card(C3, 156, COLW, 80, "AWS KMS  ·  Secrets Manager", "customer-managed key,", "envelope-wrapped data key", color=SECURITY)

# Row B — the services
rect(IX, 284, IW, 148, fill="#FFFDFA", stroke=AWS_EDGE, rx=12, sw=1.2, dash="4 4")
text(IX + 20, 308, "ECS FARGATE  ·  ONE TASK ROLE PER SERVICE", size=10.5, weight="700",
     fill="#9A5B12", spacing="0.8")

SVC = ["snapshot-loader", "cdc-applier", "repair-worker", "reconciler", "controlplane"]
SVC_SUB = ["parts → target", "stream → target", "drains the DLQ", "verifies", "cutover gate"]
sx, sw_, gap = IX + 24, 153, 12
for i, (name, sub) in enumerate(zip(SVC, SVC_SUB)):
    x = sx + i * (sw_ + gap)
    rect(x, 332, sw_, 80, fill=CARD)
    accent_bar(x + 9, 344, 56, COMPUTE)
    text(x + 20, 366, name, size=11, weight="600", family=MONO)
    text(x + 20, 384, sub, size=9.5, fill=MUTED)

# Row C — Aurora
rect(IX, 480, IW, 124, fill="#F5F9FF", stroke="#B9D2EE", rx=12)
text(IX + 20, 506, "AMAZON AURORA  ·  POSTGRESQL OR MYSQL  ·  STORAGE ENCRYPTED WITH THE CMK",
     size=10.5, weight="700", fill="#1B4E86", spacing="0.8")
card(IX + 24, 524, 398, 60, "Writer endpoint", "migrated tables + migration_ctl control schema", color=DATA)
card(IX + 442, 524, 398, 60, "Reader endpoint", "reconciler digest scans run here", color=DATA)

# Row D — observability
card(C1, 652, COLW, 80, "CloudWatch Logs", "structured JSON,", "row images structurally refused", color=MUTED)
card(C2, 652, COLW, 80, "CloudWatch Alarms", "lag · dead letters · drift —", "each mapped to a runbook", color=MUTED)
card(C3, 652, COLW, 80, "VPC Flow Logs", "who reached the database", "holding regulated data", color=MUTED)

rect(IX, 756, IW, 60, fill="#FFFDFA", stroke=HAIRLINE, rx=8)
text(IX + 18, 780, "Endpoints", size=11, weight="700", fill="#9A5B12")
text(IX + 96, 780, "S3 gateway endpoint for bulk parts; interface endpoints for KMS, Secrets Manager, "
                   "ECR, CloudWatch and STS.", size=10.5, fill=MUTED)
text(IX + 96, 798, "Aurora reads parts from S3 itself, so bulk bytes never pass through a worker "
                   "process. No workload has a route to the internet.", size=10.5, fill=MUTED)

# ------------------------------------------------------------------ arrows

# on-premise → S3 (bulk parts) and → MSK (change events), across Direct Connect
poly([(cx + cw, 386), (436, 386), (436, 196), (C1, 196)], STORAGE, width=2.4)
label(444, 180, "sealed .dat parts", STORAGE)

poly([(cx + cw, 414), (470, 414), (470, 148), (C2 + COLW / 2, 148), (C2 + COLW / 2, 156)],
     STREAM, width=2.4)
label(700, 138, "protected change events", STREAM)

# S3 → snapshot-loader (verify, stage, merge)
poly([(C1 + 100, 236), (C1 + 100, 332)], STORAGE)
label(C1 + 110, 276, "verify + load", STORAGE)

# S3 → Aurora directly: the native import path, down the left channel
poly([(C1, 208), (536, 208), (536, 554), (IX + 24, 554)], STORAGE, width=2.4, dash="6 4")
add(f'<text transform="translate(521,384) rotate(-90)" font-family="{SANS}" font-size="10" '
    f'font-weight="700" fill="{STORAGE}" text-anchor="middle" stroke="#FBFCFD" '
    f'stroke-width="3.5" paint-order="stroke">native S3 import</text>')

# MSK → cdc-applier and → repair-worker
poly([(C2 + 24, 236), (C2 + 24, 332)], STREAM)
poly([(C2 + 190, 236), (C2 + 190, 332)], STREAM)

# services → Aurora
poly([(IX + 300, 432), (IX + 300, 480)], DATA, width=2.4)
label(IX + 312, 462, "fenced write + offset, one transaction", DATA)

# reconciler → reader endpoint
poly([(IX + 24 + 3 * (sw_ + gap) + sw_ / 2, 412), (IX + 24 + 3 * (sw_ + gap) + sw_ / 2, 524)], DATA, dash="5 4")

# KMS → services
poly([(C3 + 120, 236), (C3 + 120, 284)], SECURITY)
label(C3 + 130, 268, "data key", SECURITY)

# ------------------------------------------------------------------ legend

LY = 872
rect(32, LY - 26, W - 64, 74, fill="#F7FAFC", stroke=HAIRLINE, rx=10)

legend = [
    (STORAGE, "Bulk part path", "extract → object storage → native import → fenced merge"),
    (STREAM, "Change stream", "source log → connector → broker → fenced apply"),
    (DATA, "Target writes and reads", "every write conditional on the source LSN"),
    (SECURITY, "Key material", "unwrapped once per process, never per row"),
]
lx = 56
for color, name, desc in legend:
    add(f'<line x1="{lx}" y1="{LY + 6}" x2="{lx + 30}" y2="{LY + 6}" stroke="{color}" '
        f'stroke-width="2.6" marker-end="url(#a-{color[1:]})" stroke-linecap="round"/>')
    text(lx + 40, LY + 3, name, size=11, weight="700")
    text(lx + 40, LY + 20, desc, size=10, fill=MUTED)
    lx += 352

text(W - 48, LY + 40, "Dashed = private circuit or a path that bypasses the workers entirely",
     size=10, fill=MUTED, anchor="end")

add('</svg>')

sys.stdout.write("\n".join(out) + "\n")
