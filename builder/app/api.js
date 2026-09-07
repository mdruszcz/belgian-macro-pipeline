/* BelPulse builder -- the one place that calls context.api and normalises
 * every response into a shape the rest of the shell can render a labelled
 * state from. No fetch happens anywhere else in builder/app/.
 *
 * Batch 12b (builder-ui). See history.js's header for the delivery mechanism.
 */

(function (shell) {
  "use strict";

  /* Human copy for every required error state in the batch spec. Kept next
   * to the codes they answer so the two cannot drift apart silently. */
  var MESSAGES = {
    unauthorized:
      "The builder's session token was rejected. This usually means the " +
      "server was restarted. Restart `make builder` and reopen the printed URL.",
    guard_rejected:
      "The document exceeded a resource limit the service will not say more " +
      "about (it deliberately echoes no detail on this rejection).",
    payload_too_large:
      "The document is larger than this service will store. The service " +
      "does not say by how much -- trim content and try again.",
    version_space_exhausted: "No more version slots are available for this page.",
    forbidden_origin: "This request was refused: the origin did not match the builder's own.",
    forbidden_host: "This request was refused: the builder answers on loopback only.",
    bad_request: "The request was malformed.",
    not_found: "That page or version does not exist.",
    method_not_allowed: "That action is not supported on this route.",
    validation_failed: "The document is not valid. See the errors below.",
    internal_error: "The builder could not complete that. Nothing was written.",
    network_error:
      "Could not reach the builder service. Check that `make builder` is still running.",
    numeric_fidelity:
      "This document contains a number the builder cannot edit without changing " +
      "how it is written (for example 1.0, 1e3, or an integer too large to " +
      "represent exactly in a browser). Loaded as read-only so nothing is " +
      "silently rewritten.",
  };

  function describeError(code) {
    return MESSAGES[code] || "Something went wrong and the builder does not have a better message.";
  }

  function jsonRequest(api, method, path, body) {
    var opts = { method: method };
    if (body !== undefined) {
      opts.headers = { "Content-Type": "application/json" };
      opts.body = JSON.stringify(body);
    }
    return api(path, opts).then(
      function (resp) {
        return resp.text().then(function (text) {
          return { status: resp.status, text: text };
        });
      },
      function () {
        return Promise.reject({ code: "network_error", message: MESSAGES.network_error });
      }
    );
  }

  function parseEnvelope(raw) {
    var body;
    try {
      body = JSON.parse(raw.text);
    } catch (err) {
      return {
        ok: false,
        code: "internal_error",
        message: "The builder returned something that was not valid JSON.",
        errors: [],
        status: raw.status,
      };
    }
    if (body && body.ok === true) {
      body.status = raw.status;
      return body;
    }
    var error = (body && body.error) || {};
    return {
      ok: false,
      code: error.code || "internal_error",
      message: describeError(error.code),
      serverMessage: error.message,
      errors: error.errors || [],
      status: raw.status,
    };
  }

  function call(api, method, path, body) {
    return jsonRequest(api, method, path, body).then(parseEnvelope, function (rejection) {
      return {
        ok: false,
        code: rejection.code || "network_error",
        message: rejection.message || MESSAGES.network_error,
        errors: [],
        status: 0,
      };
    });
  }

  function listPages(api) {
    return call(api, "GET", "/api/pages");
  }

  function getRegistry(api) {
    return call(api, "GET", "/api/registry");
  }

  function listVersions(api, pageId) {
    return call(api, "GET", "/api/versions?page_id=" + encodeURIComponent(pageId));
  }

  /**
   * getDocument -- the one call that must not trust JSON.parse blindly. The
   * raw response text is scanned for numeral literals that a browser cannot
   * re-serialise byte-for-byte (see model.scanNumberFidelity) BEFORE it is
   * parsed at all, because parsing is exactly the lossy step.
   */
  function getDocument(api, pageId, which) {
    var path =
      "/api/document?page_id=" + encodeURIComponent(pageId) + "&which=" + encodeURIComponent(which);
    return jsonRequest(api, "GET", path).then(function (raw) {
      if (raw.status === 200) {
        var fidelity = shell.model.scanNumberFidelity(raw.text);
        if (!fidelity.safe) {
          return {
            ok: false,
            code: "numeric_fidelity",
            message: describeError("numeric_fidelity"),
            riskyLiterals: fidelity.risky,
            status: raw.status,
          };
        }
      }
      return parseEnvelope(raw);
    });
  }

  function getPreviewHtml(api, pageId, which, lang) {
    var path =
      "/preview?page_id=" +
      encodeURIComponent(pageId) +
      "&which=" +
      encodeURIComponent(which) +
      "&lang=" +
      encodeURIComponent(lang);
    return jsonRequest(api, "GET", path).then(function (raw) {
      if (raw.status === 200) {
        return { ok: true, html: raw.text, status: raw.status };
      }
      return parseEnvelope(raw);
    });
  }

  function postValidate(api, pageId, document) {
    return call(api, "POST", "/api/validate", { page_id: pageId, document: document });
  }

  function postSave(api, pageId, document) {
    return call(api, "POST", "/api/save", { page_id: pageId, document: document });
  }

  function postPublish(api, pageId) {
    return call(api, "POST", "/api/publish", { page_id: pageId });
  }

  function postRestore(api, pageId, version) {
    return call(api, "POST", "/api/restore", { page_id: pageId, version: version });
  }

  shell.api = {
    describeError: describeError,
    listPages: listPages,
    getRegistry: getRegistry,
    listVersions: listVersions,
    getDocument: getDocument,
    getPreviewHtml: getPreviewHtml,
    postValidate: postValidate,
    postSave: postSave,
    postPublish: postPublish,
    postRestore: postRestore,
  };
})(shell);
