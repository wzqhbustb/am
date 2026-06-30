export const meta = {
  name: "deep-research",
  description: "Deep research harness - fan-out web searches, fetch sources, adversarially verify claims, synthesize a cited report.",
  whenToUse: "When the user wants a deep, multi-source, fact-checked research report on any topic. Before invoking, check whether the question is specific enough to research directly; if underspecified, ask 2-3 clarifying questions, then pass the refined question as args.",
  phases: [
    { title: "Scope", detail: "Decompose question from args into focused research angles" },
    { title: "Search", detail: "Run parallel WebSearch agents, one per angle" },
    { title: "Fetch", detail: "Deduplicate URLs, fetch top sources, extract falsifiable claims" },
    { title: "Verify", detail: "Use 3-vote adversarial verification per claim" },
    { title: "Synthesize", detail: "Merge findings, rank confidence, cite sources" }
  ]
};

const QUESTION = typeof args === "string" ? args.trim() : "";
if (!QUESTION) {
  return {
    error: "No research question provided. Pass it as args: Workflow({ name: \"deep-research\", args: \"<question>\" })."
  };
}

const ANGLES_SCHEMA = {
  type: "object",
  required: ["question", "summary", "angles"],
  properties: {
    question: { type: "string" },
    summary: { type: "string" },
    angles: {
      type: "array",
      minItems: 3,
      maxItems: 6,
      items: {
        type: "object",
        required: ["label", "query"],
        properties: {
          label: { type: "string" },
          query: { type: "string" },
          rationale: { type: "string" }
        }
      }
    }
  }
};

const SEARCH_SCHEMA = {
  type: "object",
  required: ["results"],
  properties: {
    results: {
      type: "array",
      maxItems: 6,
      items: {
        type: "object",
        required: ["url", "title", "relevance"],
        properties: {
          url: { type: "string" },
          title: { type: "string" },
          snippet: { type: "string" },
          relevance: { enum: ["high", "medium", "low"] }
        }
      }
    }
  }
};

const CLAIMS_SCHEMA = {
  type: "object",
  required: ["sourceQuality", "claims"],
  properties: {
    sourceQuality: { enum: ["primary", "secondary", "blog", "forum", "unreliable"] },
    publishDate: { type: "string" },
    claims: {
      type: "array",
      maxItems: 5,
      items: {
        type: "object",
        required: ["claim", "quote", "importance"],
        properties: {
          claim: { type: "string" },
          quote: { type: "string" },
          importance: { enum: ["central", "supporting", "tangential"] }
        }
      }
    }
  }
};

const VERDICT_SCHEMA = {
  type: "object",
  required: ["refuted", "evidence", "confidence"],
  properties: {
    refuted: { type: "boolean" },
    evidence: { type: "string" },
    confidence: { enum: ["high", "medium", "low"] },
    counterSource: { type: "string" }
  }
};

const REPORT_SCHEMA = {
  type: "object",
  required: ["summary", "findings", "caveats"],
  properties: {
    summary: { type: "string" },
    findings: {
      type: "array",
      items: {
        type: "object",
        required: ["claim", "confidence", "sources", "evidence"],
        properties: {
          claim: { type: "string" },
          confidence: { enum: ["high", "medium", "low"] },
          sources: { type: "array", items: { type: "string" } },
          evidence: { type: "string" },
          vote: { type: "string" }
        }
      }
    },
    caveats: { type: "string" },
    openQuestions: { type: "array", items: { type: "string" } }
  }
};

const VOTES_PER_CLAIM = 3;
const REFUTATIONS_REQUIRED = 2;
const MAX_FETCH = 15;
const MAX_VERIFY_CLAIMS = 25;
const relevanceRank = { high: 0, medium: 1, low: 2 };
const importanceRank = { central: 0, supporting: 1, tangential: 2 };
const qualityRank = { primary: 0, secondary: 1, blog: 2, forum: 3, unreliable: 4 };
const confidenceRank = { high: 0, medium: 1, low: 2 };

function hostOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "unknown";
  }
}

function normalizeUrl(url) {
  try {
    const parsed = new URL(url);
    return (parsed.hostname.replace(/^www\./, "") + parsed.pathname.replace(/\/$/, "")).toLowerCase();
  } catch {
    return String(url).toLowerCase();
  }
}

phase("Scope");
const scope = await agent(
  "Break this research question into complementary search angles.\n\n" +
    "Question: " + QUESTION + "\n\n" +
    "Return 3-6 specific search angles. Each angle needs a short label, a web search query, and optional rationale. " +
    "Choose angles that improve coverage, recency, authority, and skepticism. Structured output only.",
  { label: "scope", phase: "Scope", schema: ANGLES_SCHEMA }
);
if (!scope) {
  return { question: QUESTION, error: "Scope agent returned no result." };
}
log("Question: " + QUESTION.slice(0, 100));
log("Angles: " + scope.angles.map((angle) => angle.label).join(", "));

phase("Search");
const searched = await parallel(
  scope.angles.map((angle) => () =>
    agent(
      "Use WebSearch to research this angle.\n\n" +
        "Original question: " + QUESTION + "\n" +
        "Angle: " + angle.label + "\n" +
        "Search query: " + angle.query + "\n\n" +
        "Return the top 4-6 relevant results. Prefer primary, authoritative, current, and non-SEO sources. Structured output only.",
      { label: "search:" + angle.label, phase: "Search", schema: SEARCH_SCHEMA }
    ).then((result) => {
      if (!result) return null;
      log(angle.label + ": " + result.results.length + " search results");
      return { angle: angle.label, results: result.results };
    })
  )
);

phase("Fetch");
const seen = new Map();
let remainingFetches = MAX_FETCH;
const fetchTasks = [];
for (const search of searched.filter(Boolean)) {
  const sorted = [...search.results].sort((a, b) => relevanceRank[a.relevance] - relevanceRank[b.relevance]);
  for (const result of sorted) {
    const key = normalizeUrl(result.url);
    if (seen.has(key)) continue;
    if (remainingFetches <= 0) break;
    seen.set(key, { angle: search.angle, title: result.title });
    remainingFetches--;
    fetchTasks.push({ ...result, angle: search.angle });
  }
}
log("Fetching " + fetchTasks.length + " unique sources");

const sources = (await parallel(
  fetchTasks.map((source) => () =>
    agent(
      "Use WebFetch to read this source and extract checkable claims relevant to the research question.\n\n" +
        "Question: " + QUESTION + "\n" +
        "Source URL: " + source.url + "\n" +
        "Source title: " + source.title + "\n" +
        "Found via angle: " + source.angle + "\n\n" +
        "Assess source quality and extract 0-5 concrete, falsifiable claims. Include a supporting quote for each claim. " +
        "If the page fails, is irrelevant, or is paywalled, return sourceQuality=\"unreliable\" and claims=[]. Structured output only.",
      { label: "fetch:" + hostOf(source.url), phase: "Fetch", schema: CLAIMS_SCHEMA }
    ).then((result) => {
      if (!result) return null;
      return {
        url: source.url,
        title: source.title,
        angle: source.angle,
        sourceQuality: result.sourceQuality,
        publishDate: result.publishDate,
        claims: result.claims.map((claim) => ({
          ...claim,
          sourceUrl: source.url,
          sourceTitle: source.title,
          sourceQuality: result.sourceQuality
        }))
      };
    }).catch((error) => {
      log("fetch failed: " + source.url + " - " + (error && error.message ? error.message : String(error)));
      return { url: source.url, title: source.title, angle: source.angle, sourceQuality: "unreliable", claims: [] };
    })
  )
)).filter(Boolean);

const claims = sources
  .flatMap((source) => source.claims)
  .sort((a, b) =>
    (importanceRank[a.importance] - importanceRank[b.importance]) ||
    (qualityRank[a.sourceQuality] - qualityRank[b.sourceQuality])
  )
  .slice(0, MAX_VERIFY_CLAIMS);
log("Extracted " + claims.length + " claims from " + sources.length + " sources");

if (claims.length === 0) {
  return {
    question: QUESTION,
    summary: "No checkable claims were extracted from the fetched sources.",
    findings: [],
    caveats: "The search/fetch phase returned no usable claims.",
    sources: sources.map((source) => ({ url: source.url, quality: source.sourceQuality })),
    stats: { angles: scope.angles.length, sources: sources.length, claims: 0 }
  };
}

phase("Verify");
const voted = (await parallel(
  claims.map((claim) => () =>
    parallel(
      Array.from({ length: VOTES_PER_CLAIM }, (_, vote) => () =>
        agent(
          "Try to refute this claim. Default to skeptical judgment when evidence is weak.\n\n" +
            "Research question: " + QUESTION + "\n" +
            "Claim: " + claim.claim + "\n" +
            "Source: " + claim.sourceUrl + " (" + claim.sourceQuality + ")\n" +
            "Supporting quote: " + claim.quote + "\n\n" +
            "Use WebSearch if needed. Mark refuted=true when the quote does not support the claim, credible evidence contradicts it, " +
            "the source is too weak for the claim, or the claim appears outdated. Structured output only.",
          { label: "verify:" + vote + ":" + claim.claim.slice(0, 36), phase: "Verify", schema: VERDICT_SCHEMA }
        )
      )
    ).then((verdicts) => {
      const valid = verdicts.filter(Boolean);
      const refutedVotes = valid.filter((verdict) => verdict.refuted).length;
      const survives = valid.length >= REFUTATIONS_REQUIRED && refutedVotes < REFUTATIONS_REQUIRED;
      log((survives ? "kept: " : "refuted: ") + claim.claim.slice(0, 70));
      return { ...claim, verdicts: valid, refutedVotes, survives };
    })
  )
)).filter(Boolean);

const confirmed = voted.filter((claim) => claim.survives);
const refuted = voted.filter((claim) => !claim.survives);
log("Verified " + voted.length + " claims; confirmed " + confirmed.length + ", refuted " + refuted.length);

if (confirmed.length === 0) {
  return {
    question: QUESTION,
    summary: "All reviewed claims were refuted or lacked enough valid verification votes.",
    findings: [],
    caveats: "Research is inconclusive because no claim survived adversarial verification.",
    refuted: refuted.map((claim) => ({
      claim: claim.claim,
      source: claim.sourceUrl,
      vote: (claim.verdicts.length - claim.refutedVotes) + "-" + claim.refutedVotes
    })),
    sources: sources.map((source) => ({ url: source.url, quality: source.sourceQuality, claimCount: source.claims.length })),
    stats: { angles: scope.angles.length, sources: sources.length, claims: claims.length, verified: voted.length, confirmed: 0 }
  };
}

phase("Synthesize");
const evidenceBlock = confirmed.map((claim, index) => {
  const best = claim.verdicts
    .filter((verdict) => !verdict.refuted)
    .sort((a, b) => confidenceRank[a.confidence] - confidenceRank[b.confidence])[0];
  return "[" + index + "] " + claim.claim + "\n" +
    "Vote: " + (claim.verdicts.length - claim.refutedVotes) + "-" + claim.refutedVotes + "\n" +
    "Source: " + claim.sourceUrl + " (" + claim.sourceQuality + ")\n" +
    "Quote: " + claim.quote + "\n" +
    "Verifier evidence: " + (best ? best.evidence : "No supporting verifier evidence.");
}).join("\n\n");

const report = await agent(
  "Write a concise research report answering the question.\n\n" +
    "Question: " + QUESTION + "\n\n" +
    "Confirmed claims after adversarial verification:\n" + evidenceBlock + "\n\n" +
    "Merge duplicate claims, group related findings, cite source URLs, assign confidence, note caveats, and list open questions. Structured output only.",
  { label: "synthesize", phase: "Synthesize", schema: REPORT_SCHEMA }
);

if (!report) {
  return {
    question: QUESTION,
    summary: "Synthesis was skipped or failed; returning verified claims directly.",
    findings: confirmed.map((claim) => ({
      claim: claim.claim,
      confidence: "medium",
      sources: [claim.sourceUrl],
      evidence: claim.quote,
      vote: (claim.verdicts.length - claim.refutedVotes) + "-" + claim.refutedVotes
    })),
    caveats: "The final synthesis agent did not return a report.",
    refuted: refuted.map((claim) => ({ claim: claim.claim, source: claim.sourceUrl })),
    sources: sources.map((source) => ({ url: source.url, quality: source.sourceQuality, claimCount: source.claims.length }))
  };
}

return {
  question: QUESTION,
  ...report,
  refuted: refuted.map((claim) => ({
    claim: claim.claim,
    source: claim.sourceUrl,
    vote: (claim.verdicts.length - claim.refutedVotes) + "-" + claim.refutedVotes
  })),
  sources: sources.map((source) => ({ url: source.url, quality: source.sourceQuality, angle: source.angle, claimCount: source.claims.length })),
  stats: {
    angles: scope.angles.length,
    sourcesFetched: sources.length,
    claimsExtracted: claims.length,
    claimsVerified: voted.length,
    confirmed: confirmed.length,
    refuted: refuted.length,
    agentCalls: 1 + scope.angles.length + fetchTasks.length + (claims.length * VOTES_PER_CLAIM) + 1
  }
};