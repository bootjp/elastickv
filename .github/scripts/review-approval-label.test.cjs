'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { LABEL, evaluateApproval, reconcilePullRequest, run } = require('./review-approval-label.cjs');

const HEAD = 'a221d229b7aedcdc83b2063bb0e2c7373d1d45a4';
const OLD = '698f76b25404e7870a946c853aa8dac11e00ae67';
const USERS = {
  codex: { login: 'chatgpt-codex-connector[bot]', id: 199175422, type: 'Bot' },
  claude: { login: 'claude[bot]', id: 209825114, type: 'Bot' },
};
const codeResult = overrides => ({
  commit_sha: HEAD, status: 'completed', has_any_findings: false,
  has_unresolved_findings: false, ...overrides,
});
/** Build a trusted bot comment carrying structured review evidence. */
function comment(reviewer, result, overrides = {}) {
  const name = reviewer === 'codex' ? 'codex-review-state' : 'agent-review-result';
  const value = result || (reviewer === 'codex' ? { code_review: codeResult() } : codeResult());
  return {
    id: 1, user: USERS[reviewer], created_at: '2026-10-24T10:00:00Z',
    body: `<!-- ${name}:v1 ${JSON.stringify(value)} -->`, ...overrides,
  };
}
/** Build a submitted review bound to the current test HEAD. */
function review(overrides = {}) {
  return { id: 10, user: USERS.claude, commit_id: HEAD, state: 'COMMENTED', body: '', ...overrides };
}
/** Build a root inline finding, allowing stale commits and replies in cases. */
function inline(overrides = {}) {
  return { id: 20, user: USERS.claude, commit_id: HEAD, original_commit_id: HEAD, ...overrides };
}

const cases = [
  ['no reviews', {}, false],
  ['completed Codex code review', { comments: [comment('codex')] }, true],
  ['completed Claude code review', { comments: [comment('claude')] }, true],
  ['both reviewers clean', { comments: [comment('codex'), comment('claude')] }, true],
  ['ordinary no-blocking-issues prose', { comments: [comment('claude', null, { body: 'No blocking issues. One optional nit.' })] }, false],
  ['wrong bot ID', { comments: [comment('codex', null, { user: { ...USERS.codex, id: 42 } })] }, false],
  ['human masquerading as bot', { comments: [comment('claude', null, { user: { ...USERS.claude, type: 'User' } })] }, false],
  ['untrusted login', { comments: [comment('codex', null, { user: { ...USERS.codex, login: 'some-other-bot[bot]' } })] }, false],
  ['old reviewed SHA', { comments: [comment('codex', { code_review: codeResult({ commit_sha: OLD }) })] }, false],
  ['abbreviated reviewed SHA', { comments: [comment('claude', codeResult({ commit_sha: HEAD.slice(0, 10) }))] }, false],
  ['pending review', { comments: [comment('codex', { code_review: codeResult({ status: 'in_progress' }) })] }, false],
  ['failed review', { comments: [comment('claude', codeResult({ status: 'failed' }))] }, false],
  ['limited scope review', { comments: [comment('claude', codeResult({ status: 'incomplete' }))] }, false],
  ['security review alone', { comments: [comment('codex', { security_review: codeResult() })] }, false],
  ['security-only pass permits clean Claude code review', { comments: [
    comment('codex', { code_review: null, security_review: codeResult() }), comment('claude'),
  ] }, true],
  ['security-only findings veto clean Claude code review', { comments: [
    comment('codex', { code_review: null, security_review: codeResult({ has_any_findings: true }) }), comment('claude'),
  ] }, false],
  ['security findings veto clean code review', { comments: [comment('codex', {
    code_review: codeResult(), security_review: codeResult({ has_any_findings: true }),
  })] }, false],
  ['old security findings do not veto', { comments: [comment('codex', {
    code_review: codeResult(), security_review: codeResult({ commit_sha: OLD, has_any_findings: true }),
  })] }, true],
  ['malformed current security fields fail closed', { comments: [comment('codex', {
    code_review: codeResult(), security_review: codeResult({ has_any_findings: 'true' }),
  })] }, false],
  ['resolved findings still count', { comments: [comment('codex', {
    code_review: codeResult({ has_any_findings: true, has_unresolved_findings: false }),
  })] }, false],
  ['unresolved findings veto', { comments: [comment('codex', {
    code_review: codeResult({ has_unresolved_findings: true }),
  })] }, false],
  ['Claude findings veto clean Codex review', { comments: [comment('codex'), comment('claude', codeResult({ has_any_findings: true }))] }, false],
  ['old findings do not veto another current review', { comments: [comment('codex'), comment('claude', codeResult({ commit_sha: OLD, has_any_findings: true }))] }, true],
  ['strings are not booleans', { comments: [comment('claude', codeResult({ has_any_findings: 'false' }))] }, false],
  ['missing unresolved field', { comments: [comment('codex', { code_review: {
    commit_sha: HEAD, status: 'completed', has_any_findings: false,
  } })] }, false],
  ['malformed JSON', { comments: [comment('codex', null, { body: '<!-- codex-review-state:v1 {broken} -->' })] }, false],
  ['unknown marker version', { comments: [comment('claude', null, { body: '<!-- agent-review-result:v2 {} -->' })] }, false],
  ['duplicate marker', { comments: [comment('claude', null, { body: `${comment('claude').body}\n${comment('claude').body}` })] }, false],
  ['quoted marker', { comments: [comment('claude', null, { body: `> ${comment('claude').body}` })] }, false],
  ['code-fenced marker is only an example', { comments: [comment('claude', null, { body: `Example:\n\x60\x60\x60html\n${comment('claude').body}\n\x60\x60\x60` })] }, false],
  ['tilde-fenced marker is only an example', { comments: [comment('claude', null, { body: `~~~html\n${comment('claude').body}\n~~~` })] }, false],
  ['code samples before the real marker are allowed', { comments: [comment('claude', null, { body: `\x60\x60\x60go\nreturn nil\n\x60\x60\x60\n${comment('claude').body}` })] }, true],
  ['CRLF result marker', { comments: [comment('codex', null, { body: `Summary\r\n${comment('codex').body}\r\n` })] }, true],
  ['malformed result vetoes other clean reviewer', { comments: [comment('codex'), comment('claude', null, { body: '<!-- agent-review-result:v1 null -->' })] }, false],
  ['inline finding vetoes clean summary', { comments: [comment('codex')], inlineComments: [inline()] }, false],
  ['old inline finding mapped to HEAD vetoes clean summary', { comments: [comment('codex')], inlineComments: [inline({ original_commit_id: OLD })] }, false],
  ['Codex finding mapped to HEAD vetoes its clean summary', { comments: [comment('codex')], inlineComments: [inline({ user: USERS.codex, original_commit_id: OLD })] }, false],
  ['wholly stale inline finding permits clean summary', { comments: [comment('codex')], inlineComments: [inline({ commit_id: OLD, original_commit_id: OLD })] }, true],
  ['resolved historical finding permits fresh clean review', { comments: [comment('codex')], inlineComments: [inline({ original_commit_id: OLD })], resolvedCommentIds: [20] }, true],
  ['resolved Codex historical finding permits fresh clean review', { comments: [comment('codex')], inlineComments: [inline({ user: USERS.codex, original_commit_id: OLD })], resolvedCommentIds: [20] }, true],
  ['resolved original HEAD finding still vetoes approval', { comments: [comment('codex')], inlineComments: [inline()], resolvedCommentIds: [20] }, false],
  ['resolved finding with unknown origin still vetoes approval', { comments: [comment('codex')], inlineComments: [inline({ original_commit_id: null })], resolvedCommentIds: [20] }, false],
  ['resolved historical finding alone cannot establish approval', { inlineComments: [inline({ original_commit_id: OLD })], resolvedCommentIds: [20] }, false],
  ['another resolved thread cannot clear the current finding', { comments: [comment('codex')], inlineComments: [inline({ original_commit_id: OLD })], resolvedCommentIds: [99] }, false],
  ['original HEAD finding vetoes even with different mapped commit', { comments: [comment('codex')], inlineComments: [inline({ commit_id: OLD })] }, false],
  ['inline replies are not new findings', { comments: [comment('codex')], inlineComments: [inline({ in_reply_to_id: 19 })] }, true],
  ['human inline comment is outside this label', { comments: [comment('codex')], inlineComments: [inline({ user: { login: 'bootjp', id: 1306365, type: 'User' } })] }, true],
  ['change request vetoes clean summary', { comments: [comment('codex')], reviews: [review({ state: 'CHANGES_REQUESTED' })] }, false],
  ['dismissed review invalidates current HEAD', { comments: [comment('codex')], reviews: [review({ state: 'DISMISSED' })] }, false],
  ['old dismissed review does not invalidate new HEAD', { comments: [comment('codex')], reviews: [review({ state: 'DISMISSED', commit_id: OLD })] }, true],
  ['approval without explicit zero findings is insufficient', { reviews: [review({ state: 'APPROVED', body: 'LGTM, except for a nit' })] }, false],
  ['structured result in submitted review', { reviews: [review({ ...comment('claude'), state: 'APPROVED' })] }, true],
  ['review commit must agree with marker commit', { reviews: [review({ ...comment('claude'), commit_id: OLD })] }, false],
  ['unsubmitted review is insufficient', { reviews: [review({ ...comment('claude'), state: 'PENDING' })] }, false],
  ['deleted only clean result', { comments: [] }, false],
  ['latest findings replace old clean result', { comments: [
    comment('claude'), comment('claude', codeResult({ has_any_findings: true }), { id: 2, updated_at: '2026-10-24T11:00:00Z' }),
  ] }, false],
  ['latest pending result replaces old clean result', { comments: [
    comment('codex'), comment('codex', { code_review: codeResult({ status: 'in_progress' }) }, { id: 2, updated_at: '2026-10-24T11:00:00Z' }),
  ] }, false],
  ['latest malformed result does not revive old clean result', { comments: [
    comment('claude'), comment('claude', null, { id: 2, updated_at: '2026-10-24T11:00:00Z', body: '<!-- agent-review-result:v1 {broken} -->' }),
  ] }, false],
  ['edit of summary is authoritative', { comments: [
    comment('codex', { code_review: codeResult({ has_any_findings: true }) }, { id: 1, updated_at: '2026-10-24T12:00:00Z' }),
    comment('codex', null, { id: 2, updated_at: '2026-10-24T11:00:00Z' }),
  ] }, false],
  ['invalid HEAD', { head: 'not-a-sha', comments: [comment('codex')] }, false],
];
for (const [name, input, approved] of cases) {
  test(name, () => assert.equal(evaluateApproval({ head: HEAD, ...input }).approved, approved));
}

for (const reviewer of ['codex', 'claude']) {
  for (const fence of ['```', '~~~']) {
    test(`${reviewer} fenced marker example does not invalidate the real result (${fence})`, () => {
      const result = comment(reviewer);
      result.body = `${fence}html\n${result.body}\n${fence}\n${result.body}`;
      assert.equal(evaluateApproval({ head: HEAD, comments: [result] }).approved, true);
    });
    test(`${reviewer} fenced-only example does not replace clean evidence (${fence})`, () => {
      const example = comment(reviewer, null, { id: 2, updated_at: '2026-10-24T11:00:00Z' });
      example.body = `${fence}html\n${example.body}\n${fence}`;
      assert.equal(evaluateApproval({ head: HEAD, comments: [comment(reviewer), example] }).approved, true);
    });
  }
}

test('quoted marker alongside a standalone result remains ambiguous', () => {
  const result = comment('claude');
  result.body = `> ${result.body}\n${result.body}`;
  assert.equal(evaluateApproval({ head: HEAD, comments: [result] }).approved, false);
});

for (const reviewer of ['codex', 'claude']) {
  test(`late stale ${reviewer} result cannot hide current-HEAD findings`, () => {
    const result = overrides => reviewer === 'codex'
      ? { code_review: codeResult(overrides) } : codeResult(overrides);
    const comments = [
      comment(reviewer === 'codex' ? 'claude' : 'codex'),
      comment(reviewer, result({ has_any_findings: true }), { id: 2 }),
    ];
    assert.equal(evaluateApproval({ head: HEAD, comments }).approved, false);
    comments.push(comment(reviewer, result({ commit_sha: OLD }), {
      id: 3, updated_at: '2026-10-24T11:00:00Z',
    }));
    assert.equal(evaluateApproval({ head: HEAD, comments }).approved, false);
  });

  test(`late stale ${reviewer} result cannot hide current-HEAD approval`, () => {
    const result = overrides => reviewer === 'codex'
      ? { code_review: codeResult(overrides) } : codeResult(overrides);
    const comments = [
      comment(reviewer),
      comment(reviewer, result({ commit_sha: OLD, has_any_findings: true }), {
        id: 2, updated_at: '2026-10-24T11:00:00Z',
      }),
    ];
    assert.equal(evaluateApproval({ head: HEAD, comments }).approved, true);
  });
}

test('late stale code result cannot hide current-HEAD security findings', () => {
  const comments = [
    comment('claude'),
    comment('codex', { code_review: codeResult(), security_review: codeResult({ has_any_findings: true }) }),
    comment('codex', { code_review: codeResult({ commit_sha: OLD }), security_review: codeResult({ commit_sha: OLD }) }, {
      id: 2, updated_at: '2026-10-24T11:00:00Z',
    }),
  ];
  assert.equal(evaluateApproval({ head: HEAD, comments }).approved, false);
});

test('late stale security result cannot hide current-HEAD security findings', () => {
  const comments = [
    comment('codex', { code_review: codeResult(), security_review: codeResult({ has_any_findings: true }) }),
    comment('codex', { code_review: codeResult(), security_review: codeResult({ commit_sha: OLD }) }, {
      id: 2, updated_at: '2026-10-24T11:00:00Z',
    }),
  ];
  assert.equal(evaluateApproval({ head: HEAD, comments }).approved, false);
});

test('code and security results are selected independently by SHA', () => {
  const comments = [
    comment('codex', { code_review: codeResult(), security_review: codeResult({ commit_sha: OLD }) }),
    comment('codex', { code_review: codeResult({ commit_sha: OLD }), security_review: codeResult() }, {
      id: 2, updated_at: '2026-10-24T11:00:00Z',
    }),
  ];
  assert.equal(evaluateApproval({ head: HEAD, comments }).approved, true);
});

/** Model API reads and label mutations, with hooks for deterministic races. */
function fakeGitHub(options = {}) {
  const state = {
    calls: [], gets: 0, labelExists: options.labelExists ?? true,
    pr: { number: 1, state: 'open', head: { sha: HEAD }, labels: [{ name: 'keep-me' }, ...(options.labeled ? [{ name: LABEL }] : [])] },
  };
  const endpoints = {
    comments: () => {}, reviews: () => {}, inline: () => {}, pulls: () => {}, closed: () => {},
  };
  const github = {
    graphql: async (_query, variables) => {
      state.calls.push(['graphql', variables]);
      if (options.failResolution) throw new Error('Thread API unavailable');
      const pages = options.threadPages ?? [{ nodes: [], pageInfo: { hasNextPage: false } }];
      const page = structuredClone(pages[Number(variables.cursor ?? 0)]);
      if (state.reopened) page.nodes.forEach(thread => { thread.isResolved = false; });
      return { repository: { pullRequest: { reviewThreads: page } } };
    },
    rest: {
      pulls: {
        list: endpoints.pulls, listReviews: endpoints.reviews, listReviewComments: endpoints.inline,
        get: async () => {
          state.gets++;
          if (options.failGet) throw new Error('PR API unavailable');
          if (options.beforeGet) options.beforeGet(state);
          return { data: structuredClone(state.pr) };
        },
      },
      issues: {
        listComments: endpoints.comments, listForRepo: endpoints.closed,
        getLabel: async args => {
          state.calls.push(['getLabel', args]);
          if (!state.labelExists) throw Object.assign(new Error('missing'), { status: 404 });
          return { data: { name: LABEL } };
        },
        createLabel: async args => {
          state.calls.push(['createLabel', args]);
          state.labelExists = true;
        },
        addLabels: async args => {
          state.calls.push(['addLabels', args]);
          state.pr.labels.push(...args.labels.map(name => ({ name })));
          if (options.afterAdd) options.afterAdd(state);
        },
        removeLabel: async args => {
          state.calls.push(['removeLabel', args]);
          if (options.removeError) throw options.removeError;
          state.pr.labels = state.pr.labels.filter(label => label.name !== args.name);
        },
      },
    },
    paginate: async (endpoint, args) => {
      assert.equal(args.per_page, 100);
      state.calls.push(['paginate', args]);
      if (endpoint === endpoints.comments) {
        if (options.failEvidence) throw new Error('Comments API unavailable');
        return options.comments ?? [comment('codex')];
      }
      if (endpoint === endpoints.reviews) return options.reviews ?? [];
      if (endpoint === endpoints.inline) return options.inlineComments ?? [];
      if (endpoint === endpoints.pulls) return options.pulls ?? [{ number: 1 }];
      if (endpoint === endpoints.closed) return options.closed ?? [];
      assert.fail('Unknown endpoint');
    },
  };
  const core = { info: () => {}, error: () => {} };
  const args = { github, repo: { owner: 'bootjp', repo: 'elastickv' }, number: 1, core };
  return { state, args };
}

test('adds only its own label, creating it when needed', async () => {
  const { state, args } = fakeGitHub({ labelExists: false });
  assert.equal((await reconcilePullRequest(args)).approved, true);
  assert.deepEqual(state.pr.labels.map(label => label.name), ['keep-me', LABEL]);
  assert.equal(state.calls.filter(([name]) => name === 'createLabel').length, 1);
  assert.equal(state.gets, 3);
});

test('existing approval is idempotent', async () => {
  const { state, args } = fakeGitHub({ labeled: true });
  assert.equal((await reconcilePullRequest(args)).approved, true);
  assert.equal(state.calls.filter(([name]) => name === 'addLabels').length, 0);
});

test('removes a stale approval without modifying other labels', async () => {
  const { state, args } = fakeGitHub({ labeled: true, comments: [comment('claude', codeResult({ commit_sha: OLD }))] });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('late stale review does not restore a label vetoed by current-HEAD findings', async () => {
  const { state, args } = fakeGitHub({ labeled: true, comments: [
    comment('codex'),
    comment('claude', codeResult({ has_any_findings: true })),
    comment('claude', codeResult({ commit_sha: OLD }), { id: 2, updated_at: '2026-10-24T11:00:00Z' }),
  ] });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
  assert.equal(state.calls.some(([name]) => name === 'addLabels'), false);
});

test('remapped root finding revokes existing approval despite clean summary', async () => {
  const { state, args } = fakeGitHub({ labeled: true,
    inlineComments: [inline({ user: USERS.codex, original_commit_id: OLD })],
  });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
  assert.equal(state.calls.some(([name]) => name === 'addLabels'), false);
});

/** Build a page of resolved root IDs from GitHub's thread query. */
function threadPage(ids, next) {
  return { nodes: ids.map(id => ({ isResolved: true, comments: { nodes: [{ databaseId: id }] } })),
    pageInfo: { hasNextPage: next !== undefined, endCursor: next } };
}

test('reads every thread page before permitting resolved historical findings', async () => {
  const { state, args } = fakeGitHub({
    inlineComments: [inline({ original_commit_id: OLD })],
    threadPages: [threadPage([999], '1'), threadPage([20])],
  });
  assert.equal((await reconcilePullRequest(args)).approved, true);
  assert.equal(state.calls.filter(([name]) => name === 'graphql').length, 4);
});

test('thread API failure revokes existing approval', async () => {
  const { state, args } = fakeGitHub({ labeled: true, failResolution: true,
    inlineComments: [inline({ original_commit_id: OLD })],
  });
  await assert.rejects(reconcilePullRequest(args), /Thread API unavailable/);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('unknown thread resolution does not approve a historical finding', async () => {
  const page = threadPage([20]);
  delete page.nodes[0].isResolved;
  const { args } = fakeGitHub({ inlineComments: [inline({ original_commit_id: OLD })], threadPages: [page] });
  assert.equal((await reconcilePullRequest(args)).approved, false);
});

test('invalid thread pagination fails closed', async () => {
  for (const page of [{ nodes: [] }, threadPage([], '0')]) {
    const { state, args } = fakeGitHub({ labeled: true,
      inlineComments: [inline({ original_commit_id: OLD })], threadPages: [page],
    });
    await assert.rejects(reconcilePullRequest(args), /Invalid review thread/);
    assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
  }
});

test('reopened historical finding revokes approval during label write', async () => {
  const { state, args } = fakeGitHub({
    inlineComments: [inline({ original_commit_id: OLD })], threadPages: [threadPage([20])],
    afterAdd: state => { state.reopened = true; },
  });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
  assert.equal(state.calls.some(([name]) => name === 'addLabels'), true);
});

test('does not attempt to remove an absent label', async () => {
  const { state, args } = fakeGitHub({ comments: [] });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.equal(state.calls.filter(([name]) => name === 'removeLabel').length, 0);
});

test('removes approval from a closed PR', async () => {
  const { state, args } = fakeGitHub({ labeled: true });
  state.pr.state = 'closed';
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
  assert.equal(state.gets, 1);
});

test('push during evidence reads prevents approval and clears old label', async () => {
  const { state, args } = fakeGitHub({ labeled: true, beforeGet: state => {
    if (state.gets === 2) state.pr.head.sha = OLD;
  } });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
  assert.equal(state.calls.filter(([name]) => name === 'addLabels').length, 0);
});

test('push during label write rolls back approval', async () => {
  const { state, args } = fakeGitHub({ afterAdd: state => { state.pr.head.sha = OLD; } });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('closing during label write rolls back approval', async () => {
  const { state, args } = fakeGitHub({ afterAdd: state => { state.pr.state = 'closed'; } });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('findings arriving during the label write roll back approval at the same HEAD', async () => {
  const comments = [comment('codex')];
  const { state, args } = fakeGitHub({ comments, afterAdd: () => {
    comments.push(comment('codex', { code_review: codeResult({ has_any_findings: true }) }, {
      id: 2, updated_at: '2026-10-24T11:00:00Z',
    }));
  } });
  assert.equal((await reconcilePullRequest(args)).approved, false);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('evidence read failure revokes approval and reports failure', async () => {
  const { state, args } = fakeGitHub({ labeled: true, failEvidence: true });
  await assert.rejects(reconcilePullRequest(args), /Comments API unavailable/);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('initial PR read failure attempts to revoke approval', async () => {
  const { state, args } = fakeGitHub({ labeled: true, failGet: true });
  await assert.rejects(reconcilePullRequest(args), /PR API unavailable/);
  assert.deepEqual(state.pr.labels, [{ name: 'keep-me' }]);
});

test('already removed label is tolerated', async () => {
  const { args } = fakeGitHub({ labeled: true, comments: [], removeError: { status: 404 } });
  assert.equal((await reconcilePullRequest(args)).approved, false);
});

test('label write permission errors are reported', async () => {
  const { args } = fakeGitHub({ labeled: true, comments: [], removeError: new Error('Forbidden') });
  await assert.rejects(reconcilePullRequest(args), /Forbidden/);
});

test('sweep includes all open PRs, closed labeled PRs, and the triggering closed PR', async () => {
  const { args } = fakeGitHub({ pulls: [{ number: 1 }, { number: 2 }], closed: [
    { number: 3, pull_request: {} }, { number: 4 },
  ] });
  const seen = [];
  const get = args.github.rest.pulls.get;
  args.github.rest.pulls.get = async params => {
    seen.push(params.pull_number);
    return get(params);
  };
  await run({ ...args, context: { repo: args.repo, payload: { pull_request: { number: 5 } } } });
  assert.deepEqual([...new Set(seen)], [1, 2, 3, 5]);
});

test('a failed PR does not prevent reconciling other PRs', async () => {
  const { args } = fakeGitHub({ pulls: [{ number: 1 }, { number: 2 }] });
  const get = args.github.rest.pulls.get;
  let secondRead = false;
  args.github.rest.pulls.get = async params => {
    if (params.pull_number === 1) throw new Error('One PR failed');
    secondRead = true;
    return get(params);
  };
  await assert.rejects(run({ ...args, context: { repo: args.repo, payload: {} } }), /Could not reconcile PRs: 1/);
  assert.equal(secondRead, true);
});
