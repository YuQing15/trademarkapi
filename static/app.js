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
  const riskBadge = document.getElementById('risk-badge');
  const summary = document.getElementById('summary');
  const notes = document.getElementById('notes');
  const matchesList = document.getElementById('matches-list');
  const warningBox = document.getElementById('warning-box');
  const warningText = document.getElementById('warning-text');
  const manualSearchWrap = document.getElementById('manual-search-wrap');
  const manualSearchLink = document.getElementById('manual-search-link');
  const sourceLabel = document.getElementById('source-label');

  function setBadge(level) {
    riskBadge.textContent = level.toUpperCase();
    riskBadge.className = `badge ${level}`;
  }

  function regUrl(regNo) {
    if (!regNo) return '';
    const cleaned = String(regNo).trim();
    return `https://trademarks.ipo.gov.uk/ipo-tmcase/page/Results/1/${encodeURIComponent(cleaned)}`;
  }

  function titleCaseWord(s) {
    if (!s) return '';
    return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
  }

  function prettySource(source) {
    const map = {
      local_database: 'Source: local database',
      ukipo_fallback: 'Source: UKIPO fallback',
      ukipo_fallback_cache: 'Source: UKIPO fallback cache',
      no_match: 'Source: no local match found'
    };
    return map[source] || `Source: ${source || 'unknown'}`;
  }

  function renderMatch(m) {
    const el = document.createElement('div');
    el.className = 'match';
    const classes = (m.class_codes || []).join(', ') || '—';
    const statusText = m.status_display || m.status || '—';
    const active = m.active ? 'Active' : (statusText === 'Closed' ? 'Closed' : 'Inactive');
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
    const reg = m.reg_no || '—';
    const regLink = reg !== '—'
      ? `<a href="${regUrl(reg)}" target="_blank" rel="noopener noreferrer">${reg}</a>`
      : '—';
    el.innerHTML = `
      <div class="title-row">
        <h3>${m.mark_text}</h3>
        <span class="${m.active ? 'pill active' : 'pill inactive'}">${active}</span>
      </div>
      <div class="meta">Trade Mark No: ${regLink} | Owner: ${m.owner_name || '—'} | Status: ${statusText}</div>
      <div class="meta">Type: ${m.mark_type || '—'} | Classes: ${classes} | Filed age: ${age} | Similarity: ${m.similarity}</div>
      <div class="goods-box">
        <div class="goods-title">Goods & Services</div>
        ${classGuide}
        <div class="meta">${goods}</div>
      </div>
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

    const trademark = trademarkEl ? trademarkEl.value.trim() : '';
    const country = countryEl ? countryEl.value : '';
    const classes = classesEl ? classesEl.value.trim() : '';
    const include_patents = false;

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
    summary.textContent = `${data.match_count} similar marks found for "${titleCaseWord(data.trademark)}" in the UK`;
    if (sourceLabel) {
      sourceLabel.textContent = prettySource(data.result_source);
    }

    const warnings = data.warnings || [];
    if (warningBox && warningText) {
      if (warnings.length) {
        warningText.textContent = warnings[0];
        warningBox.hidden = false;
      } else {
        warningText.textContent = '';
        warningBox.hidden = true;
      }
    }

    if (manualSearchWrap && manualSearchLink) {
      if (data.ukipo_manual_search_url && warnings.length) {
        manualSearchLink.href = data.ukipo_manual_search_url;
        manualSearchLink.textContent = `Search "${data.ukipo_manual_search_term || data.trademark}" on UKIPO`;
        manualSearchWrap.hidden = false;
      } else {
        manualSearchLink.removeAttribute('href');
        manualSearchWrap.hidden = true;
      }
    }

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

    result.hidden = false;
    matches.hidden = false;

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
