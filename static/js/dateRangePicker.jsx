const { useState, useEffect, useRef } = React;

function DateRangePicker({ initialStart, initialEnd, onApply, onExpose }) {
  const [start, setStart] = useState(initialStart);
  const [end, setEnd] = useState(initialEnd);
  const [open, setOpen] = useState(false);
  const containerRef = useRef(null);

  // Format ISO date (YYYY-MM-DD) to European format (DD.MM.YYYY)
  const formatEuroDate = (isoDate) => {
    if (!isoDate) return "";
    const parts = isoDate.split("-");
    if (parts.length !== 3) return isoDate;
    return `${parts[2]}.${parts[1]}.${parts[0]}`;
  };

  useEffect(() => {
    const api = {
      open: () => setOpen(true),
      setRange: (s, e) => {
        setStart(s);
        setEnd(e);
      },
      close: () => setOpen(false),
    };
    if (onExpose) {
      onExpose(api);
    }
    return () => {
      if (onExpose) {
        onExpose(null);
      }
    };
  }, [onExpose]);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event) => {
      const container = containerRef.current;
      if (!container) return;
      if (container.contains(event.target)) return;
      setOpen(false);
    };
    const onKeyDown = (event) => {
      if (event.key === "Escape") {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("touchstart", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("touchstart", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  const applyRange = () => {
    if (start && end && onApply) {
      onApply(start, end);
    }
    setOpen(false);
  };

  return (
    <div ref={containerRef} className="date-range-picker">
      <button
        type="button"
        className="btn btn-secondary btn-sm justify-between w-full date-range-picker__trigger"
        onClick={() => setOpen(!open)}
        aria-expanded={open ? "true" : "false"}
      >
        <span className="tabular text-sm">{formatEuroDate(start)} → {formatEuroDate(end)}</span>
        <svg className="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor">
          <path d="M6 9l6 6 6-6"></path>
        </svg>
      </button>
      {open && (
        <div className="date-range-picker__popover card p-4 space-y-3">
          <label className="space-y-1 text-xs font-semibold text-slate-500">
            Start
            <input type="date" className="input" value={start} onChange={(e) => setStart(e.target.value)} />
          </label>
          <label className="space-y-1 text-xs font-semibold text-slate-500">
            End
            <input type="date" className="input" value={end} onChange={(e) => setEnd(e.target.value)} />
          </label>
          <button type="button" className="btn btn-primary btn-sm w-full" onClick={applyRange}>Apply</button>
        </div>
      )}
    </div>
  );
}

window.mountDateRangePicker = function mountDateRangePicker(targetId, options = {}) {
  const host = document.getElementById(targetId);
  if (!host || !window.ReactDOM) {
    return null;
  }
  const props = {
    initialStart: options.initialStart,
    initialEnd: options.initialEnd,
    onApply: options.onApply,
    onExpose: options.onExpose,
  };
  ReactDOM.render(<DateRangePicker {...props} />, host);
  return host;
};
