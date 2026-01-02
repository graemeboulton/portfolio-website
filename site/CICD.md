# CI/CD & Branching Setup (How This Repo Works)

This repository uses **GitHub Actions + GitHub Pages** to automatically build and deploy a **Jekyll-based portfolio site**.

The setup is intentionally designed to balance **safety**, **clarity**, and **ease of working as a solo developer**, while still following professional CI/CD practices.

---

## 1. High-level overview

- **Default branch:** `master`
- **Deployment target:** GitHub Pages
- **Build tool:** Jekyll
- **CI/CD platform:** GitHub Actions
- **Workflow file:** `.github/workflows/jekyll-pages.yml`
- **Content location:** `site/`

### What happens automatically?

| Event | What happens |
|-----|-------------|
| Push to a feature branch | Site is built and checked |
| Open a pull request to `master` | Site is built and checked |
| Merge PR into `master` | Site is built and deployed to GitHub Pages |

---

## 2. CI/CD workflow explained

### 1️⃣ Trigger conditions
The workflow runs on:
- Pull requests targeting `master`
- Merges into `master`

---

### 2️⃣ Environment setup
- Repository checkout
- Ruby setup
- Dependencies installed from `site/Gemfile`

---

### 3️⃣ Site build

```bash
bundle exec jekyll build
```

Builds the site into `site/_site`. Any failure blocks deployment.

---

### 4️⃣ Site validation

HTML is validated using **html-proofer**:
- Internal links
- HTML correctness
- Navigation integrity

---

### 5️⃣ Deployment

On successful merge to `master`:
- The site is uploaded
- GitHub Pages deploys automatically

---

## 3. Branch protection rules

The `master` branch enforces:
- Pull requests required
- CI checks required
- Signed commits
- Conversations resolved
- No force pushes

---

## 4. Branching strategy

### Golden rule
**One change = one branch = one PR**

### Recommended flow

```bash
git checkout master
git pull
git checkout -b content/my-change
```

---

## 5. Why this works

- Safe deployments
- Clean history
- Professional workflow
- No solo-dev friction

---

## 6. Summary

- `master` is always deployable
- CI/CD catches errors early
- GitHub Pages stays in sync
