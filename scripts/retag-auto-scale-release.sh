#!/usr/bin/env bash
set -euo pipefail

TAG="v1.0.0-auto-scale"
REMOTE="${REMOTE:-fork}"
REPO="${GH_REPO:-$(gh repo view --json nameWithOwner --jq '.nameWithOwner')}"
HEAD_SHA="$(git rev-parse HEAD)"
WORKFLOW_NAME="release"
MAX_RELEASE_ATTEMPTS="${MAX_RELEASE_ATTEMPTS:-5}"

clear_release_assets() {
  if gh release view "${TAG}" --repo "${REPO}" >/dev/null 2>&1; then
    mapfile -t assets < <(gh release view "${TAG}" --repo "${REPO}" --json assets --jq '.assets[].name')

    if ((${#assets[@]})); then
      echo "Deleting ${#assets[@]} release asset(s)..."
      for asset in "${assets[@]}"; do
        echo "Deleting asset: ${asset}"
        gh release delete-asset "${TAG}" "${asset}" --repo "${REPO}" --yes
      done
    else
      echo "No uploaded release assets to delete."
    fi
  else
    echo "No GitHub release found for ${TAG}; skipping release asset cleanup."
  fi
}

latest_release_run_id() {
  gh run list \
    --repo "${REPO}" \
    --workflow "${WORKFLOW_NAME}" \
    --branch "${TAG}" \
    --limit 1 \
    --json databaseId,headSha \
    --jq "map(select(.headSha == \"${HEAD_SHA}\")) | .[0].databaseId // empty"
}

wait_for_new_release_run() {
  local previous_run_id="$1"
  local run_id=""

  echo "Waiting for ${WORKFLOW_NAME} workflow to start..." >&2
  for _ in {1..60}; do
    run_id="$(latest_release_run_id)"
    if [[ -n "${run_id}" && "${run_id}" != "${previous_run_id}" ]]; then
      echo "Release workflow started: https://github.com/${REPO}/actions/runs/${run_id}" >&2
      printf '%s\n' "${run_id}"
      return 0
    fi
    sleep 5
  done

  echo "Timed out waiting for a new ${WORKFLOW_NAME} workflow run." >&2
  return 1
}

wait_for_run_completion() {
  local run_id="$1"
  local status=""
  local conclusion=""

  while true; do
    status="$(gh run view "${run_id}" --repo "${REPO}" --json status --jq '.status')"
    conclusion="$(gh run view "${run_id}" --repo "${REPO}" --json conclusion --jq '.conclusion // ""')"
    echo "Run ${run_id}: status=${status} conclusion=${conclusion:-pending}" >&2

    if [[ "${status}" == "completed" ]]; then
      printf '%s\n' "${conclusion}"
      return 0
    fi

    sleep 30
  done
}

echo "Repo: ${REPO}"
echo "Remote: ${REMOTE}"
echo "Tag: ${TAG}"
echo "New target: ${HEAD_SHA}"
echo "Max release attempts: ${MAX_RELEASE_ATTEMPTS}"

release_exists=0
if gh release view "${TAG}" --repo "${REPO}" >/dev/null 2>&1; then
  release_exists=1
fi

previous_run_id="$(latest_release_run_id)"
clear_release_assets

if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "Deleting local tag ${TAG}..."
  git tag -d "${TAG}"
else
  echo "Local tag ${TAG} is not present."
fi

if git ls-remote --exit-code --tags "${REMOTE}" "refs/tags/${TAG}" >/dev/null 2>&1; then
  echo "Deleting remote tag ${TAG} from ${REMOTE}..."
  git push "${REMOTE}" ":refs/tags/${TAG}"
else
  echo "Remote tag ${TAG} is not present on ${REMOTE}."
fi

echo "Creating local tag ${TAG} at ${HEAD_SHA}..."
git tag "${TAG}" "${HEAD_SHA}"

echo "Pushing tag ${TAG} to ${REMOTE}..."
git push "${REMOTE}" "refs/tags/${TAG}"

if ((release_exists)); then
  echo "Re-anchoring release ${TAG} to ${HEAD_SHA}..."
  gh release edit "${TAG}" \
    --repo "${REPO}" \
    --tag "${TAG}" \
    --target "${HEAD_SHA}" \
    --verify-tag \
    --draft=false \
    --latest
fi

run_id="$(wait_for_new_release_run "${previous_run_id}")"
attempt=1
while ((attempt <= MAX_RELEASE_ATTEMPTS)); do
  echo "Release attempt ${attempt}/${MAX_RELEASE_ATTEMPTS}..."
  conclusion="$(wait_for_run_completion "${run_id}")"

  if [[ "${conclusion}" == "success" ]]; then
    echo "Release workflow succeeded: https://github.com/${REPO}/actions/runs/${run_id}"
    break
  fi

  if ((attempt == MAX_RELEASE_ATTEMPTS)); then
    echo "Release workflow did not succeed after ${MAX_RELEASE_ATTEMPTS} attempt(s)." >&2
    echo "Last run: https://github.com/${REPO}/actions/runs/${run_id}" >&2
    exit 1
  fi

  echo "Release workflow concluded with ${conclusion}; clearing assets and rerunning failed jobs."
  clear_release_assets
  gh run rerun "${run_id}" --repo "${REPO}" --failed
  attempt=$((attempt + 1))
done

echo "Release URL: https://github.com/${REPO}/releases/tag/${TAG}"
echo "Done."
