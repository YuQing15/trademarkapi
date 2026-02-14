function init() {
  const NICE_CLASS_INFO = {
    "1": { title: "Chemicals", examples: "industrial chemicals, fertilizers, unprocessed plastics" },
    "2": { title: "Paints and Coatings", examples: "paints, varnishes, anti-rust products" },
    "3": { title: "Cleaning and Cosmetics", examples: "soaps, perfumes, cosmetics, detergents" },
    "4": { title: "Fuels and Lubricants", examples: "industrial oils, fuels, candles" },
    "5": { title: "Pharmaceuticals", examples: "medicines, dietary supplements, sanitary products" },
    "6": { title: "Metal Goods", examples: "metal building materials, pipes, hardware" },
    "7": { title: "Machines", examples: "engines, machine tools, industrial robots" },
    "8": { title: "Hand Tools", examples: "cutlery, razors, manual tools" },
    "9": { title: "Electronics and Software", examples: "software, apps, eyewear, electrical apparatus" },
    "10": { title: "Medical Devices", examples: "surgical instruments, prosthetics, dental tools" },
    "11": { title: "Lighting and Heating", examples: "lamps, ovens, refrigeration, sanitary units" },
    "12": { title: "Vehicles", examples: "cars, bicycles, aircraft, boats" },
    "13": { title: "Firearms", examples: "firearms, ammunition, explosives" },
    "14": { title: "Jewellery", examples: "precious metals, watches, gemstones" },
    "15": { title: "Musical Instruments", examples: "guitars, pianos, instrument accessories" },
    "16": { title: "Paper and Printed Matter", examples: "books, stationery, packaging paper" },
    "17": { title: "Rubber and Plastics", examples: "insulating materials, rubber sheets, flexible pipes" },
    "18": { title: "Leather Goods", examples: "bags, wallets, luggage, saddlery" },
    "19": { title: "Building Materials", examples: "non-metal doors, tiles, timber" },
    "20": { title: "Furniture", examples: "furniture, mirrors, plastic storage boxes" },
    "21": { title: "Household Utensils", examples: "cookware, brushes, glassware" },
    "22": { title: "Ropes and Fibers", examples: "ropes, tarpaulins, raw textile fibers" },
    "23": { title: "Yarns and Threads", examples: "sewing thread, textile yarns" },
    "24": { title: "Textiles", examples: "fabric, bed linen, towels" },
    "25": { title: "Clothing", examples: "clothing, footwear, headgear" },
    "26": { title: "Sewing Articles", examples: "lace, ribbons, buttons, artificial flowers" },
    "27": { title: "Floor Coverings", examples: "carpets, rugs, mats, wallpaper" },
    "28": { title: "Games and Toys", examples: "toys, sports equipment, gaming devices" },
    "29": { title: "Processed Foods", examples: "meat, dairy, preserved fruits and vegetables" },
    "30": { title: "Staple Foods", examples: "coffee, tea, bread, confectionery" },
    "31": { title: "Agricultural Products", examples: "fresh fruits, seeds, live animals" },
    "32": { title: "Non-Alcoholic Drinks", examples: "soft drinks, mineral water, beers" },
    "33": { title: "Alcoholic Drinks", examples: "wine, spirits, liqueurs" },
    "34": { title: "Tobacco", examples: "cigarettes, cigars, smoker articles" },
    "35": { title: "Advertising and Retail", examples: "marketing, retail services, business admin" },
    "36": { title: "Financial Services", examples: "insurance, banking, real estate services" },
    "37": { title: "Construction Services", examples: "building, repair, installation" },
    "38": { title: "Telecom Services", examples: "internet communications, broadcasting" },
    "39": { title: "Transport and Storage", examples: "shipping, delivery, travel arrangement" },
    "40": { title: "Material Treatment", examples: "recycling, metal treatment, custom manufacture" },
    "41": { title: "Education and Entertainment", examples: "training, events, publishing" },
    "42": { title: "Technology Services", examples: "software development, SaaS, scientific research" },
    "43": { title: "Food and Accommodation", examples: "restaurants, cafes, hotels" },
    "44": { title: "Medical and Beauty Services", examples: "medical clinics, veterinary, beauty salons" },
    "45": { title: "Legal and Security Services", examples: "legal services, personal security, social services" }
  };

  const form = document.getElementById('check-form');
  const checkBtn = document.getElementById('check-button');
  const statusEl = document.getElementById('js-status');
  if (!form) {
    console.error('check-form not found');
    return;
  }

  console.log('Risk checker loaded');
  if (statusEl) statusEl.textContent = 'JS loaded';

  const result = document.getElementById('result');
  const matches = document.getElementById('matches');
  const patents = document.getElementById('patents');
  const riskBadge = document.getElementById('risk-badge');
  const summary = document.getElementById('summary');
  const notes = document.getElementById('notes');
  const matchesList = document.getElementById('matches-list');
  const patentsList = document.getElementById('patents-list');

  function setBadge(level) {
    riskBadge.textContent = level.toUpperCase();
    riskBadge.className = `badge ${level}`;
  }

  function renderMatch(m) {
    const el = document.createElement('div');
    el.className = 'match';
    const classes = (m.class_codes || []).join(', ') || '—';
    const active = m.active ? 'Active' : 'Inactive';
    const age = m.age_years !== null ? `${m.age_years}y` : '—';
    const goods = m.goods_services || '—';
    const classCards = (m.class_codes || [])
      .filter((c) => NICE_CLASS_INFO[c])
      .slice(0, 4)
      .map((c) => {
        const info = NICE_CLASS_INFO[c];
        return `
          <div class="class-card">
            <div class="class-title">Class ${c} - ${info.title}</div>
            <div class="class-examples">Examples: ${info.examples}</div>
          </div>
        `;
      })
      .join('');
    const classGuide = classCards
      ? `<div class="class-guide">${classCards}</div>`
      : `<div class="class-guide-empty">No class details available for this mark.</div>`;
    el.innerHTML = `
      <div class="title-row">
        <h3>${m.mark_text}</h3>
        <span class="${m.active ? 'pill active' : 'pill inactive'}">${active}</span>
      </div>
      <div class="meta">Reg: ${m.reg_no || '—'} | Owner: ${m.owner_name || '—'} | Status: ${m.status || '—'}</div>
      <div class="meta">Type: ${m.mark_type || '—'} | Classes: ${classes} | Filed age: ${age} | Similarity: ${m.similarity}</div>
      <details>
        <summary>Goods & services</summary>
        ${classGuide}
        <div class="meta">${goods}</div>
      </details>
    `;
    return el;
  }

  function renderPatent(p) {
    const el = document.createElement('div');
    el.className = 'match';
    const ipc = [p.ipc7, p.ipc8].filter(Boolean).join(' / ') || '—';
    const age = p.age_years !== null ? `${p.age_years}y` : '—';
    const pubDates = [p.publication_a_date, p.publication_b_date].filter(Boolean).join(' / ') || '—';
    const notInForce = p.date_not_in_force || '—';
    const reason = p.reason_not_in_force || '—';
    const statusPill = p.active ? 'pill active' : 'pill inactive';
    const statusText = p.active ? 'Active' : 'Inactive';
    el.innerHTML = `
      <div class="title-row">
        <h3>${p.application_number || '—'} ${p.publication_number ? `(${p.publication_number})` : ''}</h3>
        <span class="${statusPill}">${statusText}</span>
      </div>
      <div class="meta">Applicant: ${p.applicant_name || '—'} | Status: ${p.status || '—'} | Similarity: ${p.similarity}</div>
      <div class="patent-grid">
        <div><span>IPC</span>${ipc}</div>
        <div><span>Country</span>${p.applicant_country || '—'}</div>
        <div><span>Filed Age</span>${age}</div>
      </div>
      <details>
        <summary>More patent details</summary>
        <div class="patent-grid" style="margin-top:8px;">
          <div><span>Publications</span>${pubDates}</div>
          <div><span>Not In Force</span>${notInForce}</div>
          <div><span>Reason</span>${reason}</div>
        </div>
      </details>
    `;
    return el;
  }

  async function submitForm() {
    console.log('Submitting request');

    if (checkBtn) {
      checkBtn.disabled = true;
      checkBtn.textContent = 'Checking...';
    }

    const trademarkEl = document.getElementById('trademark');
    const countryEl = document.getElementById('country');
    const classesEl = document.getElementById('classes');
    const includeEl = document.getElementById('include-patents');

    const trademark = trademarkEl ? trademarkEl.value.trim() : '';
    const country = countryEl ? countryEl.value : '';
    const classes = classesEl ? classesEl.value.trim() : '';
    const include_patents = includeEl ? includeEl.checked : false;

    if (!trademark) {
      alert('Please enter a trademark');
      if (checkBtn) {
        checkBtn.disabled = false;
        checkBtn.textContent = 'Check Risk';
      }
      return;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000);

    let res;
    try {
      res = await fetch('/check', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trademark, country, classes, include_patents }),
        signal: controller.signal
      });
    } catch (err) {
      console.error('Fetch failed', err);
      alert(`Request failed: ${err && err.message ? err.message : 'unknown error'}`);
      if (checkBtn) {
        checkBtn.disabled = false;
        checkBtn.textContent = 'Check Risk';
      }
      return;
    } finally {
      clearTimeout(timeoutId);
    }

    let data = {};
    try {
      data = await res.json();
    } catch (err) {
      alert('Server returned invalid JSON');
      if (checkBtn) {
        checkBtn.disabled = false;
        checkBtn.textContent = 'Check Risk';
      }
      return;
    }
    if (!res.ok) {
      alert(data.error || 'Request failed');
      if (checkBtn) {
        checkBtn.disabled = false;
        checkBtn.textContent = 'Check Risk';
      }
      return;
    }

    setBadge(data.risk_level);
    const patentText = data.patent_count ? ` and ${data.patent_count} related patents` : '';
    summary.textContent = `Found ${data.match_count} similar marks${patentText} for "${data.trademark}" in ${data.country}.`;
    notes.innerHTML = '';
    (data.notes || []).forEach(n => {
      const li = document.createElement('li');
      li.textContent = n;
      notes.appendChild(li);
    });

    matchesList.innerHTML = '';
    (data.similar_marks || []).forEach(m => {
      matchesList.appendChild(renderMatch(m));
    });

    patentsList.innerHTML = '';
    (data.patents || []).forEach(p => {
      patentsList.appendChild(renderPatent(p));
    });

    result.hidden = false;
    matches.hidden = false;
    patents.hidden = !(data.patents && data.patents.length);

    if (checkBtn) {
      checkBtn.disabled = false;
      checkBtn.textContent = 'Check Risk';
    }
  }

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    submitForm();
  });

  if (checkBtn) {
    checkBtn.addEventListener('click', (e) => {
      e.preventDefault();
      submitForm();
    });
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
