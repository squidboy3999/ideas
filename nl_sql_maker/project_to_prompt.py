#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

import pathspec   # pip install pathspec

DEFAULT_EXTS = [
    "py","md","rst","txt",
    "c","cc","cpp","h","hpp",
    "java","kt","kts",
    "js","mjs","cjs","ts","tsx",
    "rs","go","rb","php","sh","bash","zsh",
    "toml","ini","cfg","conf","json",
    "sql"
]
# exclude yaml/yml for now

def load_gitignore(root: Path):
    gitignore = root / ".gitignore"
    if gitignore.exists():
        with gitignore.open("r", encoding="utf-8") as f:
            return pathspec.PathSpec.from_lines("gitwildmatch", f)
    return None

def gather_files(root: Path, include_exts: set[str], spec):
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            p = Path(dirpath) / fn
            rel = p.relative_to(root)

            # Skip if excluded by .gitignore
            if spec and spec.match_file(str(rel)):
                continue

            ext = p.suffix.lower().lstrip(".")
            if ext in include_exts or (ext == "" and "" in include_exts):
                yield p

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", help="Project root directory")
    parser.add_argument("-o", "--output", default="project_context.txt")
    parser.add_argument("--ext", default=",".join(DEFAULT_EXTS))
    args = parser.parse_args()

    root = Path(args.root).resolve()
    include_exts = {e.strip().lower() for e in args.ext.split(",") if e.strip()}
    spec = load_gitignore(root)

    files = sorted(gather_files(root, include_exts, spec))

    with open(args.output, "w", encoding="utf-8") as out:
        for f in files:
            rel = f.relative_to(root)
            out.write(f"The content of {rel} is :\n")
            try:
                with f.open("r", encoding="utf-8", errors="replace") as fh:
                    out.write(fh.read())
            except Exception as e:
                out.write(f"<error reading file: {e}>\n")
            out.write("\n\n")

    print(f"Wrote {len(files)} files into {args.output}")

if __name__ == "__main__":
    main()
