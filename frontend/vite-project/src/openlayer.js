import Map from 'ol/Map.js';
import Overlay from 'ol/Overlay.js';
import View from 'ol/View.js';
import TileLayer from 'ol/layer/Tile.js';
import { fromLonLat } from 'ol/proj.js';
import OSM from 'ol/source/OSM.js';

const map = new Map({
  layers: [new TileLayer({ source: new OSM() })],
  target: 'map',
  view: new View({
    center: fromLonLat([ -103.0, 48.0]),
    zoom: 8,
  }),
});

async function loadMarkers() {
  // const res = await fetch('http://localhost:3000/api/locations');
  const res = await fetch('http://192.168.164.129:3000/api/locations');
  const locations = await res.json();

  for (const loc of locations) {
    addMarker(loc);
  }
}

function addMarker(loc) {
  const pos = fromLonLat([loc.longitude, loc.latitude]);

  // create tagging elements
  const markerEl = document.createElement('div');
  markerEl.className = 'map-marker';
  markerEl.innerHTML = '📍';
  markerEl.style.cursor = 'pointer';
  markerEl.style.fontSize = '24px';

  // create opoup
  const popupEl = document.createElement('div');
  document.body.appendChild(popupEl); // have mount to DOM first

  const popupOverlay = new Overlay({
    element: popupEl,
    positioning: 'bottom-center',
    stopEvent: true,
  });

  const markerOverlay = new Overlay({
    position: pos,
    positioning: 'center-center',
    element: markerEl,
    stopEvent: false,
  });

  map.addOverlay(markerOverlay);
  map.addOverlay(popupOverlay);

  const show = (label, val) => 
    (val === null || val === undefined || val === 'N/A' || val === '') 
      ? '' 
      : `<li><strong>${label}: </strong>${val}</li>`;

  // click the pin and popup
  markerEl.addEventListener('click', (e) => {
    e.stopPropagation();

    popupOverlay.setPosition(pos);

    let popover = bootstrap.Popover.getInstance(popupEl);
    if (popover) popover.dispose();

    let well_name = (loc.well_name === "" || loc.well_name === null || loc.well_name === "N/A") ? "Undefined Well Name" : loc.well_name;

    popover = new bootstrap.Popover(popupEl, {
      animation: false,
      container: document.body,
      html: true,
      placement: 'top',
      title: `📌 ${loc.pdf_file}`,
      content: `
        <div class="ol-popup" style="min-width: 200px;">
          <ul style="margin:0; padding-left:16px; line-height:1.8;">
            ${show('Well Name', well_name)}
            ${show('Well File No', loc.well_file_no)}
            ${show('API No', loc.api_no)}
            ${show('Operator', loc.operator)}
            ${show('Enseco Job No', loc.enseco_job_no)}
            ${show('Job Type', loc.job_type)}
            ${show('County', loc.county)}
            ${show('State', loc.state)}
            ${show('Surface Hole Location', loc.well_surface_hole_location)}
            ${show('Longitude', loc.longitude)}
            ${show('Latitude', loc.latitude)}
            ${show('Datum', loc.datum)}
            ${show('Date Stimulated', loc.date_stimulated)}
            ${show('Stimulated Formation', loc.stimulated_formation)}
            ${show('Type Treatment', loc.type_treatment)}
            ${show('Acid Pct', loc.acid_pct)}
            ${show('Lbs Proppant', loc.lbs_proppant)}
            ${show('Top Ft', loc.top_ft)}
            ${show('Bottom Ft', loc.bottom_ft)}
            ${show('Stimulation Stages', loc.stimulation_stages)}
            ${show('Volume', loc.volume ? `${loc.volume} ${loc.volume_units}` : null)}
            ${show('Max Pressure PSI', loc.max_treatment_pressure_psi)}
            ${show('Max Rate Bbls/Min', loc.max_treatment_rate_bbls_min)}
            ${show('Details', loc.details)}
            ${show('Scraped API', loc.scraped_api)}
            ${show('Scraped Operator', loc.scraped_operator)}
            ${show('Location', loc.location)}
            ${show('Well Status', loc.well_status)}
            ${show('Well Type', loc.well_type)}
            ${show('Closest City', loc.closest_city)}
            ${show('Oil Produced', loc.oil_produced)}
            ${show('Gas Produced', loc.gas_produced)}
          </ul>
        </div>
      `,
    });
    popover.show();
  });

  // click the rest area to close the popup
  map.on('click', () => {
    const popover = bootstrap.Popover.getInstance(popupEl);
    if (popover) popover.dispose();
    popupOverlay.setPosition(undefined); // hidden
  });
}

loadMarkers();