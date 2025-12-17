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

  function reserveScrollbarSpace() {
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

    document.body.removeChild(outer);

    document.documentElement.style.setProperty('--scrollbar-width', `${scrollbarWidth}px`);

    if (scrollbarWidth > 0) {
      document.body.style.paddingRight = `${scrollbarWidth}px`;
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    applyTheme(getPreferredTheme());
    syncNavPeriodLinks();
    reserveScrollbarSpace();
    checkFilterChipOverflow();

    function setMobileNavOpen(isOpen) {
      const navMobile = document.querySelector(".nav-mobile");
      if (!navMobile) return;
      const mobileMoreToggles = document.querySelectorAll("[data-mobile-more-toggle]");
      navMobile.classList.toggle("is-open", isOpen);
      navMobile.setAttribute("aria-hidden", isOpen ? "false" : "true");
      mobileMoreToggles.forEach((btn) => {
        btn.setAttribute("aria-expanded", isOpen ? "true" : "false");
      });
      document.body.style.overflow = isOpen ? "hidden" : "";
    }

    document.addEventListener("click", (event) => {
      const themeToggle = event.target.closest("[data-theme-toggle]");
      if (themeToggle) {
        toggleTheme();
        return;
      }

      const moreToggle = event.target.closest("[data-nav-more-toggle]");
      if (moreToggle) {
        const desktopMoreMenu = document.querySelector("[data-nav-more-menu]");
        if (!desktopMoreMenu) return;
        const navMore = moreToggle.closest(".nav-more");
        if (!navMore) return;
        const isOpen = !navMore.classList.contains("is-open");
        navMore.classList.toggle("is-open", isOpen);
        desktopMoreMenu.setAttribute("aria-hidden", isOpen ? "false" : "true");
        moreToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
        return;
      }

      const desktopMoreItem = event.target.closest(".nav-more-link, .nav-more-action");
      if (desktopMoreItem) {
        const desktopMoreMenu = document.querySelector("[data-nav-more-menu]");
        const desktopMoreToggle = document.querySelector("[data-nav-more-toggle]");
        const navMore = desktopMoreItem.closest(".nav-more");
        if (!desktopMoreMenu || !desktopMoreToggle || !navMore) return;
        navMore.classList.remove("is-open");
        desktopMoreMenu.setAttribute("aria-hidden", "true");
        desktopMoreToggle.setAttribute("aria-expanded", "false");
      }

      const mobileMoreToggle = event.target.closest("[data-mobile-more-toggle]");
      if (mobileMoreToggle) {
        const navMobile = document.querySelector(".nav-mobile");
        setMobileNavOpen(!(navMobile && navMobile.classList.contains("is-open")));
        return;
      }

      const navMobileClose = event.target.closest("[data-nav-mobile-close]");
      if (navMobileClose) {
        setMobileNavOpen(false);
        return;
      }

      const mobileNavLink = event.target.closest(".nav-mobile-link");
      if (mobileNavLink) {
        setMobileNavOpen(false);
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
        setMobileNavOpen(false);
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

    document.addEventListener("click", (event) => {
      const desktopMoreMenu = document.querySelector("[data-nav-more-menu]");
      const desktopMoreToggle = document.querySelector("[data-nav-more-toggle]");
      const navMore = desktopMoreToggle?.closest(".nav-more") || null;
      if (!desktopMoreMenu || !desktopMoreToggle || !navMore) return;
      if (navMore.classList.contains("is-open") && !event.target.closest(".nav-more")) {
        navMore.classList.remove("is-open");
        desktopMoreMenu.setAttribute("aria-hidden", "true");
        desktopMoreToggle.setAttribute("aria-expanded", "false");
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      const desktopMoreMenu = document.querySelector("[data-nav-more-menu]");
      const desktopMoreToggle = document.querySelector("[data-nav-more-toggle]");
      const navMore = desktopMoreToggle?.closest(".nav-more") || null;
      if (desktopMoreMenu && desktopMoreToggle && navMore && navMore.classList.contains("is-open")) {
        navMore.classList.remove("is-open");
        desktopMoreMenu.setAttribute("aria-hidden", "true");
        desktopMoreToggle.setAttribute("aria-expanded", "false");
      }
      const navMobile = document.querySelector(".nav-mobile");
      if (navMobile && navMobile.classList.contains("is-open")) {
        setMobileNavOpen(false);
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

  function checkFilterChipOverflow() {
    document.querySelectorAll('.transactions-filter-chips').forEach(chips => {
      if (chips.closest('#dashboard-filter-card')) return;

      const hasOverflow = chips.scrollWidth > chips.clientWidth;
      chips.classList.toggle('has-overflow', hasOverflow);
    });
  }

  window.addEventListener('resize', () => {
    checkFilterChipOverflow();
  });

  document.addEventListener("htmx:afterSwap", () => {
    applyTheme(getPreferredTheme());
    syncNavPeriodLinks();
    reserveScrollbarSpace();
    checkFilterChipOverflow();
    const navMobile = document.querySelector(".nav-mobile");
    navMobile?.classList.remove("is-open");
    navMobile?.setAttribute("aria-hidden", "true");
    document.querySelectorAll("[data-mobile-more-toggle]").forEach((btn) => {
      btn.setAttribute("aria-expanded", "false");
    });
    const desktopMoreMenu = document.querySelector("[data-nav-more-menu]");
    const desktopMoreToggle = document.querySelector("[data-nav-more-toggle]");
    const navMore = desktopMoreToggle?.closest(".nav-more") || null;
    navMore?.classList.remove("is-open");
    desktopMoreMenu?.setAttribute("aria-hidden", "true");
    desktopMoreToggle?.setAttribute("aria-expanded", "false");
    document.body.style.overflow = "";
  });

  window.addEventListener("popstate", () => {
    syncNavPeriodLinks();
  });
    if (window.mountDateRangePicker) {
        // ... existing date picker logic ...
    }

    class TagInput {
        constructor(container) {
            this.container = container;
            this.hiddenInput = container.querySelector('input[type="hidden"]');
            this.textInput = container.querySelector('input[type="text"]');
            this.chipsContainer = container.querySelector('.tag-chips');
            this.suggestionsContainer = container.querySelector('.tag-suggestions');
            this.availableTags = JSON.parse(container.dataset.availableTags || '[]');
            this.selectedTags = new Set(this.hiddenInput.value.split(',').map(t => t.trim()).filter(t => t));

            this.init();
        }

        init() {
            this.renderChips();
            this.renderSuggestions();

            this.textInput.addEventListener('input', () => this.renderSuggestions());
            this.textInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    this.addTag(this.textInput.value);
                    this.textInput.value = '';
                    this.renderSuggestions();
                } else if (e.key === 'Backspace' && !this.textInput.value && this.selectedTags.size > 0) {
                    const lastTag = Array.from(this.selectedTags).pop();
                    this.removeTag(lastTag);
                }
            });
        }

        addTag(name) {
            const cleanName = name.trim();
            if (!cleanName || this.selectedTags.has(cleanName)) return;
            
            this.selectedTags.add(cleanName);
            this.updateHiddenInput();
            this.renderChips();
            this.renderSuggestions(); // Update suggestions to hide selected
        }

        removeTag(name) {
            this.selectedTags.delete(name);
            this.updateHiddenInput();
            this.renderChips();
            this.renderSuggestions();
        }

        updateHiddenInput() {
            this.hiddenInput.value = Array.from(this.selectedTags).join(',');
        }

        renderChips() {
            this.chipsContainer.innerHTML = '';
            this.selectedTags.forEach(tag => {
                const chip = document.createElement('div');
                chip.className = 'tag-chip';
                chip.innerHTML = `
                    <span>${tag}</span>
                    <button type="button" class="tag-chip-remove" aria-label="Remove tag">
                        <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
                    </button>
                `;
                chip.querySelector('button').addEventListener('click', (e) => {
                    e.stopPropagation(); // Prevent focusing input if we click delete
                    this.removeTag(tag);
                });
                this.chipsContainer.appendChild(chip);
            });
        }

        renderSuggestions() {
            const query = this.textInput.value.toLowerCase();
            const suggestions = this.availableTags
                .filter(tag => !this.selectedTags.has(tag.name))
                .filter(tag => tag.name.toLowerCase().includes(query));

            this.suggestionsContainer.innerHTML = '';
            
            // Only show suggestions if there's a query OR we have some initial/common tags to show
            // (Showing all might be overwhelming, but let's stick to showing all matching for now)
            
            suggestions.forEach(tag => {
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'tag-suggestion';
                btn.textContent = tag.name;
                btn.addEventListener('click', () => {
                    this.addTag(tag.name);
                    this.textInput.value = ''; // Clear input immediately
                    this.textInput.focus();
                });
                this.suggestionsContainer.appendChild(btn);
            });
            
            // "Create new" prompt
            if (query && !suggestions.some(t => t.name.toLowerCase() === query)) {
                 const createBtn = document.createElement('button');
                 createBtn.type = 'button';
                 createBtn.className = 'tag-suggestion tag-suggestion--create';
                 createBtn.innerHTML = `Create "<strong>${this.textInput.value}</strong>"`;
                 createBtn.addEventListener('click', () => {
                     this.addTag(this.textInput.value);
                     this.textInput.value = '';
                     this.textInput.focus();
                 });
                 this.suggestionsContainer.appendChild(createBtn);
            }
        }
    }

    document.querySelectorAll('.tag-input-container').forEach(el => new TagInput(el));
    
    // Re-init on HTMX content swaps (like modal open)
    document.addEventListener('htmx:afterSwap', (evt) => {
        evt.target.querySelectorAll('.tag-input-container').forEach(el => new TagInput(el));
    });

})();
