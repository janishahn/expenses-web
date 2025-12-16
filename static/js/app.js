(function () {
  const THEME_STORAGE_KEY = "expenses-theme";
  const docEl = document.documentElement;

  const icons = {
    success:
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M9 12l2 2 4-4"/><circle cx="12" cy="12" r="9"/></svg>',
    error:
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M15 9l-6 6"/><path d="M9 9l6 6"/><circle cx="12" cy="12" r="9"/></svg>',
    info:
      '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 8h.01"/><path d="M11 12h1v4h1"/><circle cx="12" cy="12" r="9"/></svg>',
  };

  function getPreferredTheme() {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "light" || stored === "dark") {
      return stored;
    }
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }

  function applyTheme(theme) {
    docEl.setAttribute("data-theme", theme);
    docEl.classList.toggle("dark", theme === "dark");
    localStorage.setItem(THEME_STORAGE_KEY, theme);
    document.querySelectorAll("[data-theme-toggle]").forEach((btn) => {
      btn.setAttribute("aria-pressed", theme === "dark");
    });
  }

  function toggleTheme() {
    const current = docEl.getAttribute("data-theme") || "light";
    applyTheme(current === "light" ? "dark" : "light");
  }

  function ensureToastRoot() {
    let root = document.getElementById("toast-root");
    if (!root) {
      root = document.createElement("div");
      root.id = "toast-root";
      root.className = "toast-region";
      root.setAttribute("role", "status");
      root.setAttribute("aria-live", "polite");
      document.body.appendChild(root);
    }
    return root;
  }

  function showToast(message, variant = "success") {
    const root = ensureToastRoot();
    const toast = document.createElement("div");
    toast.className = "toast";
    toast.dataset.variant = variant;
    const icon = icons[variant] || icons.info;
    toast.innerHTML = `${icon}<div>${message}</div>`;
    root.appendChild(toast);
    setTimeout(() => toast.classList.add("fade"), 3500);
    setTimeout(() => toast.remove(), 4200);
  }

  function httpErrorMessage(xhr) {
    if (!xhr) return "Request failed";
    const text = xhr.responseText || "";
    try {
      const payload = JSON.parse(text);
      if (typeof payload?.detail === "string" && payload.detail.trim()) {
        return payload.detail;
      }
    } catch (_) {
      // ignore
    }
    if (text.trim()) return text;
    return `Request failed (${xhr.status})`;
  }

  function openDialog(modal) {
    if (!modal) return;
    if (typeof modal.showModal === "function") {
      modal.showModal();
    } else {
      modal.setAttribute("open", "open");
    }
  }

  function closeDialog(modal) {
    if (!modal) return;
    if (typeof modal.close === "function") {
      modal.close();
    } else {
      modal.removeAttribute("open");
    }
  }

  function centsToAmountInput(cents) {
    const num = Number(cents);
    if (!Number.isFinite(num)) return "";
    const sign = num < 0 ? "-" : "";
    const abs = Math.abs(num);
    const euros = Math.floor(abs / 100);
    const remainder = abs % 100;
    const centsStr = String(remainder).padStart(2, "0");
    return `${sign}${euros}.${centsStr}`;
  }

  function toLocalDateTimeInputValue(date) {
    const pad = (n) => String(n).padStart(2, "0");
    const yyyy = date.getFullYear();
    const mm = pad(date.getMonth() + 1);
    const dd = pad(date.getDate());
    const hh = pad(date.getHours());
    const min = pad(date.getMinutes());
    return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
  }

  function syncNavPeriodLinks() {
    const params = new URLSearchParams(window.location.search);
    const period = params.get("period");
    const start = params.get("start");
    const end = params.get("end");
    const next = new URLSearchParams();
    if (period) next.set("period", period);
    if (start) next.set("start", start);
    if (end) next.set("end", end);
    const qs = next.toString();
    document.querySelectorAll("[data-nav-base]").forEach((link) => {
      const base = link.getAttribute("data-nav-base");
      if (!base) return;
      link.setAttribute("href", qs ? `${base}?${qs}` : base);
    });
  }

  window.ui = {
    toggleTheme,
    showToast,
    httpErrorMessage,
    syncNavPeriodLinks,
  };

  // Function to reserve space for scrollbar to prevent layout shift
  function reserveScrollbarSpace() {
    // Create a temporary element to measure scrollbar width
    const outer = document.createElement('div');
    outer.style.overflow = 'scroll';
    outer.style.visibility = 'hidden';
    outer.style.position = 'absolute';
    outer.style.top = '-9999px';
    outer.style.left = '-9999px';
    outer.style.width = '100px';
    outer.style.height = '100px';
    document.body.appendChild(outer);

    const inner = document.createElement('div');
    inner.style.width = '100%';
    inner.style.height = '100px';
    outer.appendChild(inner);

    const scrollbarWidth = outer.offsetWidth - inner.offsetWidth;

    // Clean up
    document.body.removeChild(outer);

    // Set CSS custom property for scrollbar width
    document.documentElement.style.setProperty('--scrollbar-width', `${scrollbarWidth}px`);

    // If scrollbar exists, reserve space for it
    if (scrollbarWidth > 0) {
      document.body.style.paddingRight = `${scrollbarWidth}px`;
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    applyTheme(getPreferredTheme());
    syncNavPeriodLinks();
    reserveScrollbarSpace();

    document.addEventListener("click", (event) => {
      const themeToggle = event.target.closest("[data-theme-toggle]");
      if (themeToggle) {
        toggleTheme();
        return;
      }

      const menuToggle = event.target.closest(".mobile-menu-toggle");
      if (menuToggle) {
        const navMobile = document.querySelector(".nav-mobile");
        const isOpen = navMobile?.classList.toggle("is-open");
        menuToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
        return;
      }

      const mobileNavLink = event.target.closest(".nav-mobile-link");
      if (mobileNavLink) {
        const navMobile = document.querySelector(".nav-mobile");
        const menuToggle = document.querySelector(".mobile-menu-toggle");
        navMobile?.classList.remove("is-open");
        menuToggle?.setAttribute("aria-expanded", "false");
      }

      const txnClose = event.target.closest("[data-close-transaction]");
      if (txnClose) {
        closeDialog(document.getElementById("transaction-modal"));
        return;
      }

      const txnTrigger = event.target.closest("[data-open-transaction]");
      if (txnTrigger) {
        const modal = document.getElementById("transaction-modal");
        const form = modal?.querySelector("form");
        if (form) {
          form.reset();
          const end = document.querySelector("#dashboard-filter-form input[name='end']")?.value;
          const occurredAtInput = form.querySelector("input[name='occurred_at']");
          if (occurredAtInput) {
            const when = new Date();
            if (end) {
              const [y, m, d] = end.split("-").map((v) => parseInt(v, 10));
              if (y && m && d) {
                when.setFullYear(y, m - 1, d);
              }
            }
            occurredAtInput.value = toLocalDateTimeInputValue(when);
          }
        }
        openDialog(modal);
        window.setTimeout(() => {
          const first = modal?.querySelector('input[name="amount"], input, select, textarea, button');
          first?.focus?.();
        }, 0);
        return;
      }

      const categoryClose = event.target.closest("[data-close-category]");
      if (categoryClose) {
        closeDialog(document.getElementById("category-modal"));
        return;
      }

      const categoryTrigger = event.target.closest("[data-open-category]");
      if (categoryTrigger) {
        const modal = document.getElementById("category-modal");
        const form = modal?.querySelector("form");
        if (form) {
          form.reset();
        }
        openDialog(modal);
        window.setTimeout(() => {
          const first = modal?.querySelector('input[name="name"], input, select, textarea, button');
          first?.focus?.();
        }, 0);
        return;
      }

      const balanceAnchorClose = event.target.closest("[data-close-balance-anchor]");
      if (balanceAnchorClose) {
        closeDialog(document.getElementById("balance-anchor-modal"));
        return;
      }

      const balanceAnchorEditClose = event.target.closest(
        "[data-close-balance-anchor-edit]"
      );
      if (balanceAnchorEditClose) {
        closeDialog(document.getElementById("balance-anchor-edit-modal"));
        return;
      }

      const balanceAnchorTrigger = event.target.closest("[data-open-balance-anchor]");
      if (balanceAnchorTrigger) {
        const modal = document.getElementById("balance-anchor-modal");
        const form = modal?.querySelector("form");
        if (form) {
          form.reset();
          const asOfAtInput = form.querySelector("input[name='as_of_at']");
          if (asOfAtInput) {
            asOfAtInput.value = toLocalDateTimeInputValue(new Date());
          }
        }
        openDialog(modal);
        window.setTimeout(() => {
          const first = modal?.querySelector('input[name="balance"], input, select, textarea, button');
          first?.focus?.();
        }, 0);
        return;
      }

      const balanceAnchorEditTrigger = event.target.closest(
        "[data-open-balance-anchor-edit]"
      );
      if (balanceAnchorEditTrigger) {
        const modal = document.getElementById("balance-anchor-edit-modal");
        const form = document.getElementById("balance-anchor-edit-form");
        if (form) {
          const anchorId = balanceAnchorEditTrigger.getAttribute("data-anchor-id");
          const anchorAt = balanceAnchorEditTrigger.getAttribute("data-anchor-at");
          const balanceCents = balanceAnchorEditTrigger.getAttribute(
            "data-anchor-balance-cents"
          );
          const note =
            balanceAnchorEditTrigger.getAttribute("data-anchor-note") || "";
          if (anchorId) {
            const action = `/balance-anchors/${encodeURIComponent(anchorId)}/edit`;
            form.setAttribute("action", action);
            form.setAttribute("hx-post", action);
          }
          const asOfAtInput = form.querySelector('input[name="as_of_at"]');
          const balanceInput = form.querySelector('input[name="balance"]');
          const noteInput = form.querySelector('input[name="note"]');
          if (asOfAtInput && anchorAt) asOfAtInput.value = anchorAt;
          if (balanceInput) balanceInput.value = centsToAmountInput(balanceCents);
          if (noteInput) noteInput.value = note;
        }
        openDialog(modal);
        window.setTimeout(() => {
          const first = modal?.querySelector(
            'input[name="balance"], input, select, textarea, button'
          );
          first?.focus?.();
        }, 0);
        return;
      }

      const reportTrigger = event.target.closest("[data-open-report]");
      if (reportTrigger) {
        const modal = document.getElementById("report-modal");
        const form = modal?.querySelector("form");
        if (form) {
          const dashboardForm = document.getElementById("dashboard-filter-form");
          const start = dashboardForm?.elements?.["start"]?.value;
          const end = dashboardForm?.elements?.["end"]?.value;
          const startInput = form.querySelector("input[name='start']");
          const endInput = form.querySelector("input[name='end']");
          const advancedLink = form.querySelector("[data-report-advanced]");
          if (start && startInput) {
            startInput.value = start;
          }
          if (end && endInput) {
            endInput.value = end;
          }
          if (start && end && advancedLink instanceof HTMLAnchorElement) {
            advancedLink.href = `/reports/builder?period=custom&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`;
          }
        }
        openDialog(modal);
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" || event.shiftKey || event.isComposing) return;
      const modal = document.getElementById("transaction-modal");
      if (!modal || !modal.hasAttribute("open")) return;
      const target = event.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.closest("textarea")) return;
      if (target.closest("button")) return;
      const form = modal.querySelector("form");
      if (!form) return;
      event.preventDefault();
      const submit = form.querySelector('button[type="submit"]');
      if (typeof form.requestSubmit === "function") {
        form.requestSubmit(submit || undefined);
      } else {
        form.submit();
      }
    });
  });

  // Check for filter chip overflow and add indicator class
  function checkFilterChipOverflow() {
    document.querySelectorAll('.transactions-filter-chips').forEach(chips => {
      // Skip dashboard filter chips (they wrap)
      if (chips.closest('#dashboard-filter-card')) return;

      const hasOverflow = chips.scrollWidth > chips.clientWidth;
      chips.classList.toggle('has-overflow', hasOverflow);
    });
  }



  // Run on load and after HTMX swaps
  document.addEventListener("DOMContentLoaded", () => {
    checkFilterChipOverflow();
  });

  window.addEventListener('resize', () => {
    checkFilterChipOverflow();
  });

  document.addEventListener("htmx:afterSwap", () => {
    applyTheme(getPreferredTheme());
    syncNavPeriodLinks();
    reserveScrollbarSpace();
    checkFilterChipOverflow();
    const navMobile = document.querySelector(".nav-mobile");
    const menuToggle = document.querySelector(".mobile-menu-toggle");
    navMobile?.classList.remove("is-open");
    menuToggle?.setAttribute("aria-expanded", "false");
  });

  window.addEventListener("popstate", () => {
    syncNavPeriodLinks();
  });
})();
