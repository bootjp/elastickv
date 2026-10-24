'use strict';

const LABEL = 'agent-review-approved';
const BOTS = {
  'chatgpt-codex-connector[bot]': { id: 199175422, reviewer: 'codex' },
  'claude[bot]': { id: 209825114, reviewer: 'claude' },
};

/** Identify a reviewer only when both the bot account and numeric ID match. */
function reviewerOf(item) {
  const bot = BOTS[item.user?.login];
  return bot && item.user.id === bot.id && item.user.type === 'Bot'
    ? bot.reviewer : undefined;
}

/** Parse one standalone versioned result, ignoring examples in code fences. */
function marker(body, name) {
  if (typeof body !== 'string' || !body.includes(`<!-- ${name}:`)) return;
  // A quoted example inside a code fence is not a review result.
  let fence;
  const prose = body.split(/\r?\n/).filter(line => {
    const match = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
    if (match) {
      if (!fence) fence = match[1];
      else if (match[1][0] === fence[0] && match[1].length >= fence.length && !match[2].trim()) fence = undefined;
      return false;
    }
    return !fence;
  }).join('\n');
  const matches = [...prose.matchAll(new RegExp(`^<!-- ${name}:v1 (.+) -->[ \\t]*$`, 'gm'))];
  if (matches.length !== 1 || prose.split(`<!-- ${name}:`).length !== 2) return null;
  try {
    const value = JSON.parse(matches[0][1]);
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
  } catch {
    return null;
  }
}

/** Select the most recently updated result, breaking timestamp ties by ID. */
function latest(items) {
  return items.toSorted((a, b) => {
    const time = item => Date.parse(item.updated_at || item.submitted_at || item.created_at) || 0;
    return time(b.item) - time(a.item) || b.item.id - a.item.id;
  })[0];
}

/** Keep the requested review kind for HEAD, retaining unreadable evidence. */
function currentResults(results, reviewer, kind, head) {
  return results.flatMap(result => {
    // An unreadable marker cannot establish which SHA it describes.
    if (!result.value) return [result];
    const value = reviewer === 'codex' ? result.value[kind] : result.value;
    if (value == null) return [];
    // Filter by SHA before choosing the latest result. A late stale review
    // must not replace either approval or findings for the current HEAD.
    if (/^[0-9a-f]{40}$/.test(value.commit_sha || '') && value.commit_sha !== head) return [];
    return [{ item: result.item, value }];
  });
}

/** Decide whether explicit current-HEAD evidence establishes a clean review. */
function evaluateApproval({ head, comments = [], reviews = [], inlineComments = [] }) {
  if (!/^[0-9a-f]{40}$/.test(head || '')) return { approved: false, reason: 'Invalid HEAD SHA' };
  const trustedReviews = reviews.filter(review => reviewerOf(review) && review.commit_id === head);
  if (trustedReviews.some(review => ['CHANGES_REQUESTED', 'DISMISSED'].includes(review.state))) {
    return { approved: false, reason: 'Current-HEAD review requests changes or was dismissed' };
  }
  if (inlineComments.some(comment => reviewerOf(comment) && !comment.in_reply_to_id &&
      (comment.original_commit_id || comment.commit_id) === head)) {
    return { approved: false, reason: 'Current-HEAD inline findings exist' };
  }

  const results = { codex: [], claude: [] };
  for (const item of [...comments, ...reviews.filter(review => ['COMMENTED', 'APPROVED'].includes(review.state))]) {
    const reviewer = reviewerOf(item);
    if (!reviewer) continue;
    const value = marker(item.body, reviewer === 'codex' ? 'codex-review-state' : 'agent-review-result');
    if (value !== undefined) results[reviewer].push({ item, value });
  }

  const approvedBy = [];
  for (const reviewer of ['codex', 'claude']) {
    // Code and security can finish for different SHAs in the same summary.
    const securityResult = reviewer === 'codex'
      ? latest(currentResults(results[reviewer], reviewer, 'security_review', head)) : undefined;
    if (securityResult) {
      const security = securityResult.value;
      if (!security || security.commit_sha !== head ||
          typeof security.has_any_findings !== 'boolean' ||
          typeof security.has_unresolved_findings !== 'boolean') {
        return { approved: false, reason: 'Invalid current-HEAD security fields' };
      }
      if (security.has_any_findings || security.has_unresolved_findings) {
        return { approved: false, reason: 'Current-HEAD security findings exist' };
      }
    }
    const result = latest(currentResults(results[reviewer], reviewer, 'code_review', head));
    if (!result) continue;
    if (!result.value) return { approved: false, reason: `Invalid ${reviewer} result marker` };
    const code = result.value;
    if (!/^[0-9a-f]{40}$/.test(code.commit_sha || '')) {
      return { approved: false, reason: `Missing ${reviewer} code-review SHA` };
    }
    if (result.item.commit_id && result.item.commit_id !== code.commit_sha) {
      return { approved: false, reason: `Mismatched ${reviewer} review commit` };
    }
    if (typeof code.has_any_findings !== 'boolean' || typeof code.status !== 'string' ||
        (reviewer === 'codex' && typeof code.has_unresolved_findings !== 'boolean')) {
      return { approved: false, reason: `Invalid ${reviewer} code-review fields` };
    }
    if (code.has_any_findings || code.has_unresolved_findings === true) {
      return { approved: false, reason: `Current-HEAD ${reviewer} findings exist` };
    }
    if (code.status === 'completed') approvedBy.push(reviewer);
  }
  return {
    approved: approvedBy.length > 0,
    reason: approvedBy.length ? `Zero findings at HEAD: ${approvedBy.join(', ')}` : 'No completed zero-finding code review at HEAD',
  };
}

/** Check for the managed label without depending on other PR labels. */
function hasLabel(pr) {
  return pr.labels.some(label => (typeof label === 'string' ? label : label.name) === LABEL);
}

/** Remove only the managed label; an already absent label needs no action. */
async function removeLabel(github, repo, number) {
  try {
    await github.rest.issues.removeLabel({ ...repo, issue_number: number, name: LABEL });
  } catch (error) {
    if (error.status !== 404) throw error;
  }
}

/** Create the repository label if absent, tolerating concurrent creation. */
async function ensureLabel(github, repo) {
  try {
    await github.rest.issues.getLabel({ ...repo, name: LABEL });
  } catch (error) {
    if (error.status !== 404) throw error;
    try {
      await github.rest.issues.createLabel({
        ...repo, name: LABEL, color: '0e8a16',
        description: 'Current HEAD has a completed code review with zero findings',
      });
    } catch (createError) {
      if (createError.status !== 422) throw createError;
      // Another run may have created the same label.
      await github.rest.issues.getLabel({ ...repo, name: LABEL });
    }
  }
}

/** Read every page of public review evidence through the injected API client. */
async function readEvidence(github, repo, number) {
  const params = { ...repo, pull_number: number, per_page: 100 };
  const [comments, reviews, inlineComments] = await Promise.all([
    github.paginate(github.rest.issues.listComments, { ...repo, issue_number: number, per_page: 100 }),
    github.paginate(github.rest.pulls.listReviews, params),
    github.paginate(github.rest.pulls.listReviewComments, params),
  ]);
  return { comments, reviews, inlineComments };
}

/** Reconcile one PR and revoke approval if its HEAD, evidence, or state changes. */
async function reconcilePullRequest({ github, repo, number, core }) {
  const params = { ...repo, pull_number: number };
  try {
    const { data: pr } = await github.rest.pulls.get(params);
    if (pr.state !== 'open') {
      if (hasLabel(pr)) await removeLabel(github, repo, number);
      return { approved: false, reason: 'PR is closed' };
    }
    const evidence = await readEvidence(github, repo, number);
    let decision = evaluateApproval({ head: pr.head.sha, ...evidence });
    const { data: current } = await github.rest.pulls.get(params);
    if (current.head.sha !== pr.head.sha || current.state !== 'open') {
      decision.approved = false;
      decision.reason = 'HEAD or PR state changed during reconciliation';
    }
    if (!decision.approved) {
      if (hasLabel(current)) await removeLabel(github, repo, number);
    } else {
      await ensureLabel(github, repo);
      if (!hasLabel(current)) {
        await github.rest.issues.addLabels({ ...repo, issue_number: number, labels: [LABEL] });
      }
      const [afterEvidence, { data: after }] = await Promise.all([
        readEvidence(github, repo, number), github.rest.pulls.get(params),
      ]);
      if (after.head.sha !== pr.head.sha || after.state !== 'open') {
        await removeLabel(github, repo, number);
        decision.approved = false;
        decision.reason = 'HEAD or PR state changed while applying label';
      } else {
        decision = evaluateApproval({ head: after.head.sha, ...afterEvidence });
        if (!decision.approved) await removeLabel(github, repo, number);
      }
    }
    core.info(`#${number}: ${decision.reason}`);
    return decision;
  } catch (error) {
    // Do not keep an old approval when its evidence could not be verified.
    await removeLabel(github, repo, number);
    throw error;
  }
}

/** Sweep open and closed labeled PRs, reporting failures after processing all. */
async function run({ github, context, core }) {
  const repo = context.repo;
  const [pulls, closedLabeled] = await Promise.all([
    github.paginate(github.rest.pulls.list, { ...repo, state: 'open', per_page: 100 }),
    github.paginate(github.rest.issues.listForRepo, { ...repo, state: 'closed', labels: LABEL, per_page: 100 }),
  ]);
  const numbers = new Set(pulls.map(pr => pr.number));
  for (const issue of closedLabeled) {
    if (issue.pull_request) numbers.add(issue.number);
  }
  // Include a closed PR whose event triggered this run so its label is removed.
  const eventNumber = context.payload.pull_request?.number ||
    (context.payload.issue?.pull_request && context.payload.issue.number);
  if (eventNumber) numbers.add(eventNumber);
  const failures = [];
  for (const number of numbers) {
    try {
      await reconcilePullRequest({ github, repo, number, core });
    } catch (error) {
      core.error(`#${number}: ${error.message}`);
      failures.push(number);
    }
  }
  if (failures.length) throw new Error(`Could not reconcile PRs: ${failures.join(', ')}`);
}

module.exports = { LABEL, evaluateApproval, reconcilePullRequest, run };
