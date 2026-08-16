# Pushing this repository to GitHub

The repository is already initialised with one commit on `main`. Nothing has been
pushed anywhere.

## 1. Create an empty repository

On GitHub, create a new repository named `db-migration-platform`. **Do not** let
it add a README, a .gitignore or a licence — the repository already has all three
and an initialised repo would conflict.

## 2. Push

```bash
cd db-migration-platform

git remote add origin git@github.com:<your-username>/db-migration-platform.git
git push -u origin main
```

Over HTTPS instead of SSH:

```bash
git remote add origin https://github.com/<your-username>/db-migration-platform.git
git push -u origin main
```

## 3. Adjust the module path if your username differs

The Go module path is `github.com/udaykishore-resu/db-migration-platform`. If your
GitHub username is different, rewrite it before pushing so that `go get` works for
anyone who clones it:

```bash
OLD=github.com/udaykishore-resu/db-migration-platform
NEW=github.com/<your-username>/db-migration-platform

grep -rl "$OLD" --include='*.go' --include='go.mod' --include='*.md' . \
  | xargs sed -i '' -e "s#$OLD#$NEW#g"     # macOS
# on Linux: xargs sed -i -e "s#$OLD#$NEW#g"

go build ./... && go test ./...
git commit -am "Update module path"
```

## 4. Confirm it builds from a clean clone

```bash
cd /tmp && git clone git@github.com:<your-username>/db-migration-platform.git
cd db-migration-platform && make test && make build
```

Unit tests need no databases, no Kafka and no cloud credentials.

## Notes

- **`git commit --amend --reset-author`** before pushing if you would rather the
  commit carry only your identity.
- **GitHub Actions** runs on the first push: build, vet, race tests, lint, and an
  integration job that stands up real PostgreSQL and MySQL containers and verifies
  the generated SQL against both.
- **`.env`** is gitignored. `.env.example` is committed and holds no secrets.
