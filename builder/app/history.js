/* BelPulse builder -- undo/redo history over whole document states.
 *
 * Batch 12a, owned by builder-core. builder-ui CALLS this and does not modify
 * it; Batch 13 EXTENDS it through the transaction surface below rather than
 * replacing it.
 *
 * Delivery. There is no static-file route in this design. `bootstrap_html()`
 * in src/builder/service.py reads this file at request time and concatenates it
 * into one inline <script> closure, where a `shell` namespace object and an
 * `api(path, options)` closure are already in scope. So this file attaches to
 * `shell` and declares no global of its own. To load it on its own (a test),
 * wrap it in a scope that defines `const shell = {};` first.
 *
 * WHAT IS STORED, AND WHY IT MATTERS
 *
 * A history entry holds a WHOLE DOCUMENT STATE as plain, cloned data -- never a
 * DOM node, never a reference into the live editor model, never a diff. Two
 * reasons, both learned the expensive way in editors that did the other thing:
 *
 *  - A DOM reference in an undo stack is a reference to something the next
 *    render deletes. Undo then restores a node that is no longer in the page,
 *    and the failure shows up as "undo did nothing" long after the cause.
 *  - Whole states make save/reload and undo/redo agree by construction. What
 *    undo restores is exactly what save would have written. A diff-based stack
 *    has to replay correctly to get the same answer, and a single wrong replay
 *    silently corrupts a document -- the invisible loss the batch spec calls
 *    worse than no builder at all.
 *
 * Every state is deep-cloned on the way IN and on the way OUT, so a caller that
 * mutates the object it got back cannot reach into the stack and change the
 * past.
 *
 * VOCABULARY (stated because "head" and "tail" read backwards to half of us)
 *
 *   head = the NEWEST state, the one currently on screen. Never dropped.
 *   tail = the OLDEST state, the far end of the undo history. Dropped first
 *          when a bound is hit.
 *
 * Dropping from the tail means a long session loses its oldest undo steps and
 * keeps its recent ones. Dropping from the head would throw away the document
 * the user is looking at, which is data loss, not memory management.
 *
 * NUMBERS IN THIS FILE
 *
 * The only numeric constants here are stack and memory bounds and one
 * coalescing time window. There is no indicator value, no commune figure and no
 * mockup number in this file, and none may ever be added (claude.md rule 36).
 */

(function (shell) {
  "use strict";

  /* --- bounds ---------------------------------------------------------- */

  /* Undo depth. The batch requires at least 50 states; this is the default
   * ceiling on how many entries the stack keeps, tail dropped first. */
  var DEFAULT_MAX_ENTRIES = 100;

  /* Memory ceiling for the whole stack, in characters of serialised state.
   * A page document is capped by the server well below this, so the ceiling
   * only bites on a deep history of large documents -- which is exactly when an
   * unbounded stack would start costing the operator their tab. */
  var DEFAULT_MAX_CHARS = 8 * 1024 * 1024;

  /* Entries kept no matter what the memory ceiling says. The head alone is not
   * enough: with one entry there is nothing to undo TO, so a single large
   * document would silently disable undo. Two keeps one step of undo alive. */
  var MIN_ENTRIES = 2;

  /* Coalescing window, in milliseconds. Two pushes carrying the same
   * `coalesceKey` closer together than this become one undo step, so typing a
   * title is one undo, not one undo per keystroke. */
  var DEFAULT_COALESCE_MS = 500;

  /* --- helpers --------------------------------------------------------- */

  function clone(state) {
    if (typeof structuredClone === "function") {
      return structuredClone(state);
    }
    /* Fallback for a browser without structuredClone. Document states are
     * JSON already -- they came from and go back to the API -- so this is
     * lossless for them, and it throws loudly on anything that is not (a DOM
     * node, a function, a cycle) rather than storing a broken state. */
    return JSON.parse(JSON.stringify(state));
  }

  function weigh(state) {
    try {
      return JSON.stringify(state).length;
    } catch (err) {
      /* Unweighable means unstorable; treat it as if it filled the budget so
       * the stack sheds rather than grows without a bound it can see. */
      return DEFAULT_MAX_CHARS;
    }
  }

  function positiveInt(value, fallback) {
    return typeof value === "number" && isFinite(value) && value >= 1
      ? Math.floor(value)
      : fallback;
  }

  /* --- the stack ------------------------------------------------------- */

  /**
   * createHistory(initialState, options) -> history
   *
   * options:
   *   maxEntries   number  entry ceiling               (default 100, min 2)
   *   maxChars     number  serialised-size ceiling     (default 8 MiB)
   *   coalesceMs   number  same-key coalescing window  (default 500)
   *   now          fn ->   millisecond clock, injectable so a test does not
   *                        have to sleep to prove coalescing
   *
   * Public surface:
   *   push(state, options) -> boolean   options: {coalesceKey, label}
   *   undo()               -> state | null
   *   redo()               -> state | null
   *   canUndo()            -> boolean
   *   canRedo()            -> boolean
   *   current()            -> state | null
   *   reset(state)         -> void      forget everything; new baseline
   *   begin(label)         -> token     transaction: see below
   *   commit(token, state) -> boolean
   *   abort(token)         -> void
   *   subscribe(fn)        -> unsubscribe
   *   stats()              -> {entries, index, chars, maxEntries, maxChars,
   *                            droppedFromTail, inTransaction}
   */
  function createHistory(initialState, options) {
    var opts = options || {};
    var maxEntries = Math.max(MIN_ENTRIES, positiveInt(opts.maxEntries, DEFAULT_MAX_ENTRIES));
    var maxChars = positiveInt(opts.maxChars, DEFAULT_MAX_CHARS);
    var coalesceMs = typeof opts.coalesceMs === "number" && opts.coalesceMs >= 0
      ? opts.coalesceMs
      : DEFAULT_COALESCE_MS;
    var now = typeof opts.now === "function" ? opts.now : function () { return Date.now(); };

    /* entries[0] is the tail (oldest), entries[entries.length - 1] the newest
     * state ever pushed. `index` points at the state currently shown; anything
     * after it is redoable. */
    var entries = [];
    var index = -1;
    var chars = 0;
    var droppedFromTail = 0;
    var listeners = [];
    var openToken = null;

    function entryFor(state, meta) {
      var stored = clone(state);
      return {
        state: stored,
        chars: weigh(stored),
        coalesceKey: meta && meta.coalesceKey ? String(meta.coalesceKey) : null,
        label: meta && meta.label ? String(meta.label) : null,
        at: now(),
      };
    }

    function notify() {
      for (var i = 0; i < listeners.length; i += 1) {
        try {
          listeners[i]();
        } catch (err) {
          /* One broken listener must not break undo for the others, and must
           * not leave the stack half-updated. */
        }
      }
    }

    function dropRedoTail() {
      /* A push after an undo abandons the redo branch. This is the ONLY place
       * states are discarded from the front end of the stack, and it discards
       * a future the user chose to leave, never a past. */
      while (entries.length > index + 1) {
        chars -= entries.pop().chars;
      }
    }

    function enforceBounds() {
      /* Drop from the TAIL (index 0, the oldest state) only. */
      while (entries.length > maxEntries && entries.length > MIN_ENTRIES) {
        chars -= entries.shift().chars;
        index -= 1;
        droppedFromTail += 1;
      }
      while (chars > maxChars && entries.length > MIN_ENTRIES) {
        chars -= entries.shift().chars;
        index -= 1;
        droppedFromTail += 1;
      }
      if (index < 0) {
        index = 0;
      }
    }

    function replaceHead(entry) {
      chars -= entries[index].chars;
      entries[index] = entry;
      chars += entry.chars;
    }

    function appendEntry(entry) {
      dropRedoTail();
      entries.push(entry);
      chars += entry.chars;
      index = entries.length - 1;
      enforceBounds();
    }

    function push(state, meta) {
      if (openToken !== null) {
        /* Inside a transaction the intermediate states are deliberately not
         * recorded -- that is what a transaction is for. */
        return false;
      }
      var entry = entryFor(state, meta);
      if (
        entry.coalesceKey !== null &&
        index >= 0 &&
        entries[index].coalesceKey === entry.coalesceKey &&
        entry.at - entries[index].at <= coalesceMs
      ) {
        /* Same field, still typing: fold into the current step. The folded
         * entry keeps the ORIGINAL timestamp, so holding a key down cannot
         * roll the window forward for ever and collapse a whole session into
         * one undo step. */
        var at = entries[index].at;
        replaceHead(entry);
        entries[index].at = at;
        dropRedoTail();
        notify();
        return true;
      }
      appendEntry(entry);
      notify();
      return true;
    }

    function undo() {
      if (!canUndo()) {
        return null;
      }
      index -= 1;
      notify();
      return clone(entries[index].state);
    }

    function redo() {
      if (!canRedo()) {
        return null;
      }
      index += 1;
      notify();
      return clone(entries[index].state);
    }

    function canUndo() {
      return openToken === null && index > 0;
    }

    function canRedo() {
      return openToken === null && index >= 0 && index < entries.length - 1;
    }

    function current() {
      return index >= 0 ? clone(entries[index].state) : null;
    }

    function reset(state) {
      entries = [];
      index = -1;
      chars = 0;
      droppedFromTail = 0;
      openToken = null;
      if (state !== undefined) {
        appendEntry(entryFor(state, { label: "reset" }));
      }
      notify();
    }

    /* --- transactions: the Batch 13 extension point -------------------- *
     *
     * A pointer drag or a grid resize produces a state per pointermove event.
     * Recording each one would make a single drag cost dozens of undo steps,
     * and the user would press undo expecting the block to go back where it
     * was. So Batch 13 wraps a drag:
     *
     *     var t = history.begin("move block");
     *     ...   // live model updates, previewed, NOT pushed
     *     history.commit(t, documentState);   // one undo step
     *     // or, on Escape / an invalid drop:
     *     history.abort(t);                   // no undo step at all
     *
     * While a transaction is open, push() is refused and canUndo()/canRedo()
     * report false, so a stray keyboard shortcut mid-drag cannot interleave a
     * half-finished state into the stack. The token makes a commit from a
     * transaction that already ended a no-op rather than a corruption, which
     * is what a cancelled drag that fires one last event looks like.
     *
     * Batch 13 needs no change to any function above this comment.
     */

    function begin(label) {
      if (openToken !== null) {
        return null;
      }
      openToken = { label: label ? String(label) : null, at: now() };
      notify();
      return openToken;
    }

    function commit(token, state) {
      if (openToken === null || token !== openToken) {
        return false;
      }
      var label = openToken.label;
      openToken = null;
      appendEntry(entryFor(state, { label: label }));
      notify();
      return true;
    }

    function abort(token) {
      if (openToken === null || token !== openToken) {
        return;
      }
      openToken = null;
      notify();
    }

    function subscribe(listener) {
      if (typeof listener !== "function") {
        return function () {};
      }
      listeners.push(listener);
      return function () {
        var at = listeners.indexOf(listener);
        if (at >= 0) {
          listeners.splice(at, 1);
        }
      };
    }

    function stats() {
      return {
        entries: entries.length,
        index: index,
        chars: chars,
        maxEntries: maxEntries,
        maxChars: maxChars,
        droppedFromTail: droppedFromTail,
        inTransaction: openToken !== null,
      };
    }

    var history = {
      push: push,
      undo: undo,
      redo: redo,
      canUndo: canUndo,
      canRedo: canRedo,
      current: current,
      reset: reset,
      begin: begin,
      commit: commit,
      abort: abort,
      subscribe: subscribe,
      stats: stats,
    };

    if (initialState !== undefined) {
      appendEntry(entryFor(initialState, { label: "initial" }));
    }
    return history;
  }

  shell.createHistory = createHistory;
  shell.historyBounds = {
    maxEntries: DEFAULT_MAX_ENTRIES,
    maxChars: DEFAULT_MAX_CHARS,
    minEntries: MIN_ENTRIES,
    coalesceMs: DEFAULT_COALESCE_MS,
  };
})(shell);
