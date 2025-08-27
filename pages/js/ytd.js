(async function () {
  const fmtUSD = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });

  // Load data
  const [cube, picks] = await Promise.all([
    fetch("./data/ytd_2024_2025.json").then(r => r.json()),
    fetch("./data/picklists.json").then(r => r.json()).catch(() => ({institutions:[], ics:[], mechanisms:[], activity_codes:[], meta:{}})),
  ]);

  // Elements
  const elInst = document.getElementById("inst");
  const elICs  = document.getElementById("ics");
  const elMech = document.getElementById("mech");
  const elAct  = document.getElementById("act");
  const elRenew= document.getElementById("inclRenewal");
  const elSupp = document.getElementById("inclSupp");
  const elK2025= document.getElementById("k2025");
  const elK2024= document.getElementById("k2024");
  const elKPerc= document.getElementById("kperc");
  const elMeta = document.getElementById("metaNote");

  // Populate controls
  function opt(text, value) { const o=document.createElement("option"); o.textContent=text; o.value=value ?? text; return o; }

  elInst.append(opt("(All institutions)", ""));
  picks.institutions.forEach(x => elInst.append(opt(x, x)));

  picks.ics.forEach(x => elICs.append(opt(x, x)));
  picks.mechanisms.forEach(x => elMech.append(opt(x, x)));
  picks.activity_codes.forEach(x => elAct.append(opt(x, x)));

  if (picks.meta && picks.meta.cutoff_2025) {
    elMeta.textContent = `YTD through ${picks.meta.cutoff_2025} (2024 cutoff matched by month/day).`;
  }

  // Helpers
  function selectedMulti(sel) {
    return Array.from(sel.selectedOptions).map(o => o.value);
  }

  function filterRows() {
    const inst = elInst.value.trim();
    const ics = new Set(selectedMulti(elICs));
    const mechs = new Set(selectedMulti(elMech));
    const acts = new Set(selectedMulti(elAct));
    const inclRenewal = elRenew.checked;
    const inclSupp = elSupp.checked;

    // allowed types default
    const allowedTypes = new Set(["new"]);
    if (inclRenewal) allowedTypes.add("competing_renewal");
    if (inclSupp) allowedTypes.add("supplement");

    return cube.filter(r => {
      if (!allowedTypes.has((r.type_category || "other"))) return false;
      if (inst && (r.org_name_norm || "") !== inst) return false;
      if (ics.size && !ics.has(r.admin_ic || "")) return false;
      if (mechs.size && !mechs.has(r.mechanism || "")) return false;
      if (acts.size && !acts.has(r.activity_code || "")) return false;
      return true;
    });
  }

  function groupWeekly(rows) {
    // returns { "2024": Map(week_start_date -> amount), "2025": ... }
    const out = { "2024": new Map(), "2025": new Map() };
    for (const r of rows) {
      const y = String(r.year);
      if (y !== "2024" && y !== "2025") continue;
      const key = r.week_start; // ISO string "YYYY-MM-DD"
      const prev = out[y].get(key) || 0;
      out[y].set(key, prev + Number(r.amount || 0));
    }
    // sort by date and build cumulative arrays
    function cum(map) {
      const entries = Array.from(map.entries()).sort((a,b)=> a[0] < b[0] ? 1*-1 : (a[0] > b[0] ? 1 : 0));
      let run = 0;
      const xs = [], ys = [];
      for (const [d, v] of entries) {
        run += v;
        xs.push(d);
        ys.push(run);
      }
      return {x: xs, y: ys, total: run};
    }
    return { "2024": cum(out["2024"]), "2025": cum(out["2025"]) };
  }

  function render() {
    const rows = filterRows();
    const g = groupWeekly(rows);

    const t25 = g["2025"].total || 0;
    const t24 = g["2024"].total || 0;

    elK2025.textContent = fmtUSD.format(t25);
    elK2024.textContent = fmtUSD.format(t24);
    elKPerc.textContent = t24 > 0 ? `${Math.round((t25 / t24) * 100)}%` : (t25 > 0 ? "—" : "—");

    const traces = [];
    if (g["2025"].x.length) {
      traces.push({
        x: g["2025"].x, y: g["2025"].y,
        mode: "lines", name: "2025 YTD", line: { width: 3 }
      });
    }
    if (g["2024"].x.length) {
      traces.push({
        x: g["2024"].x, y: g["2024"].y,
        mode: "lines", name: "2024 YTD", line: { width: 2, dash: "dot" }
      });
    }

    const layout = {
      margin: {l:60,r:20,t:10,b:60},
      xaxis: { title: "Week (start date)", type: "date" },
      yaxis: { title: "Cumulative award dollars (current $)", tickformat: "~s" },
      legend: { orientation: "h", y: -0.2 }
    };

    Plotly.newPlot("chart", traces, layout, {displayModeBar:false, responsive:true});
  }

  // Wire events
  [elInst, elICs, elMech, elAct, elRenew, elSupp].forEach(el => {
    el.addEventListener("change", render);
  });

  // Initial render
  render();
})();
