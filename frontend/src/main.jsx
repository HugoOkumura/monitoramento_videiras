import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Droplets, Thermometer, CloudRain, Sprout, AlertTriangle, CheckCircle2, RefreshCw } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer, BarChart, Bar, Legend } from 'recharts';
import './styles.css';

const INFLUX_URL = import.meta.env.VITE_INFLUX_URL || 'http://localhost:8086';
const INFLUX_ORG = import.meta.env.VITE_INFLUX_ORG || 'vineyard';
const INFLUX_BUCKET = import.meta.env.VITE_INFLUX_BUCKET || 'telemetry';
const INFLUX_TOKEN = import.meta.env.VITE_INFLUX_TOKEN || '';
const BHC_MEASUREMENT = import.meta.env.VITE_BHC_MEASUREMENT || 'daily_climate_bhc';

const ETO_METHODS = {
  pm: {
    label: 'PM-FAO56',
    description: 'Penman-Monteith FAO56',
    suffix: 'pm',
    etoField: 'eto_daily_pm',
  },
  hm: {
    label: 'Hargreaves-Samani',
    description: 'Estimativa HM',
    suffix: 'hm',
    etoField: 'eto_daily_hm',
  },
  api: {
    label: 'Open-Meteo API',
    description: 'ETo da API meteorológica',
    suffix: 'api',
    etoField: 'eto_api',
  },
};

const CAD_CONFIGS = [
  { value: 75, label: 'CAD 75 mm' },
  { value: 100, label: 'CAD 100 mm' },
  { value: 150, label: 'CAD 150 mm' },
];

function parseAnnotatedCsv(text) {
  const lines = text.split('\n').filter((line) => line.trim() && !line.startsWith('#'));
  if (lines.length < 2) return [];

  const headers = lines[0].split(',');
  return lines.slice(1).map((line) => {
    const values = line.split(',');
    const row = {};
    headers.forEach((h, i) => {
      const key = h.trim();
      const raw = values[i]?.trim();
      if (raw === undefined || raw === '') row[key] = null;
      else if (!Number.isNaN(Number(raw)) && key !== '_time') row[key] = Number(raw);
      else row[key] = raw;
    });
    return row;
  });
}

async function queryInflux() {
  if (!INFLUX_TOKEN || INFLUX_TOKEN === 'PUT_YOUR_TOKEN_HERE') {
    throw new Error('Configure VITE_INFLUX_TOKEN in your .env file.');
  }

  const fields = [
    'precipitation',
    'eto_daily_pm',
    'eto_daily_hm',
    'eto_api',

    'arm_cad75_pm',
    'arm_cad100_pm',
    'arm_cad150_pm',
    'etr_cad75_pm',
    'etr_cad100_pm',
    'etr_cad150_pm',
    'alt_cad75_pm',
    'alt_cad100_pm',
    'alt_cad150_pm',
    'exc_cad75_pm',
    'exc_cad100_pm',
    'exc_cad150_pm',
    'def_cad75_pm',
    'def_cad100_pm',
    'def_cad150_pm',
    'neg_acum_cad75_pm',
    'neg_acum_cad100_pm',
    'neg_acum_cad150_pm',

    'arm_cad75_hm',
    'arm_cad100_hm',
    'arm_cad150_hm',
    'etr_cad75_hm',
    'etr_cad100_hm',
    'etr_cad150_hm',
    'alt_cad75_hm',
    'alt_cad100_hm',
    'alt_cad150_hm',
    'exc_cad75_hm',
    'exc_cad100_hm',
    'exc_cad150_hm',
    'def_cad75_hm',
    'def_cad100_hm',
    'def_cad150_hm',
    'neg_acum_cad75_hm',
    'neg_acum_cad100_hm',
    'neg_acum_cad150_hm',

    'arm_cad75_api',
    'arm_cad100_api',
    'arm_cad150_api',
    'etr_cad75_api',
    'etr_cad100_api',
    'etr_cad150_api',
    'alt_cad75_api',
    'alt_cad100_api',
    'alt_cad150_api',
    'exc_cad75_api',
    'exc_cad100_api',
    'exc_cad150_api',
    'def_cad75_api',
    'def_cad100_api',
    'def_cad150_api',
    'neg_acum_cad75_api',
    'neg_acum_cad100_api',
    'neg_acum_cad150_api',
  ];

  const flux = `
from(bucket: "${INFLUX_BUCKET}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "${BHC_MEASUREMENT}")
  |> filter(fn: (r) => contains(value: r._field, set: ${JSON.stringify(fields)}))
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"])
`;

  const res = await fetch(`${INFLUX_URL}/api/v2/query?org=${encodeURIComponent(INFLUX_ORG)}`, {
    method: 'POST',
    headers: {
      Authorization: `Token ${INFLUX_TOKEN}`,
      'Content-Type': 'application/vnd.flux',
      Accept: 'application/csv',
    },
    body: flux,
  });

  if (!res.ok) {
    const msg = await res.text();
    throw new Error(`InfluxDB query failed: ${res.status} ${msg}`);
  }

  const csv = await res.text();
  return parseAnnotatedCsv(csv).map((row) => ({
    ...row,
    time: row._time,
    date: row._time ? new Date(row._time).toLocaleDateString('pt-BR') : '-',
    precipitation: row.precipitation ?? null,
    eto_daily_pm: row.eto_daily_pm ?? null,
    eto_daily_hm: row.eto_daily_hm ?? null,
    eto_api: row.eto_api ?? null,

    arm_cad75_pm: row.arm_cad75_pm ?? null,
    arm_cad100_pm: row.arm_cad100_pm ?? null,
    arm_cad150_pm: row.arm_cad150_pm ?? null,
    etr_cad75_pm: row.etr_cad75_pm ?? null,
    etr_cad100_pm: row.etr_cad100_pm ?? null,
    etr_cad150_pm: row.etr_cad150_pm ?? null,

    arm_cad75_hm: row.arm_cad75_hm ?? null,
    arm_cad100_hm: row.arm_cad100_hm ?? null,
    arm_cad150_hm: row.arm_cad150_hm ?? null,
    etr_cad75_hm: row.etr_cad75_hm ?? null,
    etr_cad100_hm: row.etr_cad100_hm ?? null,
    etr_cad150_hm: row.etr_cad150_hm ?? null,

    arm_cad75_api: row.arm_cad75_api ?? null,
    arm_cad100_api: row.arm_cad100_api ?? null,
    arm_cad150_api: row.arm_cad150_api ?? null,
    etr_cad75_api: row.etr_cad75_api ?? null,
    etr_cad100_api: row.etr_cad100_api ?? null,
    etr_cad150_api: row.etr_cad150_api ?? null,
  }));
}

function getField(method, metric, cad = null) {
  const suffix = ETO_METHODS[method].suffix;
  if (metric === 'eto') return ETO_METHODS[method].etoField;
  return `${metric}_cad${cad}_${suffix}`;
}

function formatMm(value) {
  return value != null ? `${Number(value).toFixed(2)} mm` : '-';
}

function classifyArm(arm, cad, exc = 0) {
  if (arm == null || cad == null) {
    return {
      label: 'Sem dados',
      level: 'neutral',
      percent: null,
      text: 'Aguardando leituras de armazenamento hídrico para esta CAD.',
    };
  }

  const excess = Number(exc ?? 0);
  const percent = Math.max(0, Math.min(100, (Number(arm) / Number(cad)) * 100));

  if (excess > 0) {
    return {
      label: 'Excesso de água',
      level: 'excess',
      percent,
      text: `Foi estimado excedente hídrico de ${excess.toFixed(2)} mm. Avalie drenagem, encharcamento e risco de doenças associadas à umidade.`,
    };
  }

  if (percent < 40) {
    return {
      label: 'Crítico',
      level: 'danger',
      percent,
      text: 'Baixa reserva de água em relação à capacidade máxima do solo. Priorize avaliação de irrigação.',
    };
  }

  if (percent < 70) {
    return {
      label: 'Atenção',
      level: 'warning',
      percent,
      text: 'Reserva hídrica intermediária. Acompanhe a tendência e planeje irrigação se não houver chuva.',
    };
  }

  return {
    label: 'Estável',
    level: 'ok',
    percent,
    text: 'Boa disponibilidade de água em relação à capacidade máxima do solo.',
  };
}

function buildRecommendations(latest, method) {
  if (!latest) return ['Sem dados suficientes para gerar recomendações.'];

  const methodLabel = ETO_METHODS[method].label;
  const etoField = getField(method, 'eto');
  const precipitation = latest.precipitation ?? 0;
  const eto = latest[etoField] ?? 0;
  const deficit = Math.max(0, eto - precipitation);

  const cadStatuses = CAD_CONFIGS.map(({ value }) => {
    const armField = getField(method, 'arm', value);
    const excField = getField(method, 'exc', value);
    const arm = latest[armField];
    const exc = latest[excField] ?? 0;
    return { cad: value, arm, exc, ...classifyArm(arm, value, exc) };
  });

  const excess = cadStatuses.filter((item) => item.level === 'excess');
  const critical = cadStatuses.filter((item) => item.level === 'danger');
  const warning = cadStatuses.filter((item) => item.level === 'warning');
  const recs = [];

  if (excess.length > 0) {
    recs.push(
      `Pelo método ${methodLabel}, há excedente hídrico (${excess.map((item) => `CAD ${item.cad}: ${Number(item.exc).toFixed(2)} mm`).join(', ')}). Avalie a necessidade de drenagem no campo.`
    );
    recs.push('Verificar pontos de encharcamento, compactação do solo, escoamento superficial e risco de doenças favorecidas por excesso de umidade.');
  }

  if (critical.length > 0) {
    recs.push(
      `Pelo método ${methodLabel}, há CAD em estado crítico (${critical.map((item) => `CAD ${item.cad}: ${item.percent?.toFixed(0)}%`).join(', ')}). Priorize irrigação e inspeção visual das videiras.`
    );
    recs.push('Verificar sinais de estresse hídrico, como murcha, enrolamento de folhas, redução de vigor e queda no desenvolvimento dos cachos.');
  } else if (warning.length > 0) {
    recs.push(
      `Pelo método ${methodLabel}, há CAD em atenção (${warning.map((item) => `CAD ${item.cad}: ${item.percent?.toFixed(0)}%`).join(', ')}). Planeje irrigação preventiva se não houver chuva suficiente.`
    );
    recs.push('Acompanhar a tendência do ARM e da ETo nos próximos registros para evitar entrada em déficit hídrico.');
  } else {
    recs.push(`Pelo método ${methodLabel}, os valores de ARM estão estáveis em relação às capacidades máximas avaliadas.`);
  }

  if (deficit > 0) {
    recs.push(`Déficit hídrico estimado de ${deficit.toFixed(2)} mm: considere compensar essa diferença no manejo da irrigação.`);
  }

  if (precipitation > 20) {
    recs.push('Chuva elevada registrada: reforçar avaliação de drenagem, encharcamento e risco de doenças associadas à umidade.');
  }

  return recs;
}

function Card({ title, value, icon: Icon, subtitle }) {
  return (
    <div className="card">
      <div className="card-header">
        <span>{title}</span>
        <Icon size={20} />
      </div>
      <strong>{value}</strong>
      <small>{subtitle}</small>
    </div>
  );
}

function CadStatusCard({ cad, arm, exc, methodLabel, date }) {
  const status = classifyArm(arm, cad, exc);
  const percentLabel = status.percent != null ? `${status.percent.toFixed(0)}% da CAD` : 'Sem percentual';

  return (
    <article className={`cad-status-card ${status.level}`}>
      <div className="cad-status-top">
        {status.level === 'ok' ? <CheckCircle2 /> : <AlertTriangle />}
        <div>
          <span>Status {cad} mm</span>
          <strong>{status.label}</strong>
        </div>
      </div>
      <div className="cad-status-value">
        <strong>{formatMm(arm)}</strong>
        <small>de {cad} mm · {percentLabel}</small>
        {Number(exc ?? 0) > 0 && <small className="excess-label">Excedente: {formatMm(exc)}</small>}
      </div>
      <p>{status.text}</p>
      <small>Último registro: {date || 'sem dados'} · Método: {methodLabel}</small>
    </article>
  );
}

function MethodTabs({ activeMethod, onChange }) {
  return (
    <nav className="method-tabs" aria-label="Selecionar método de evapotranspiração">
      {Object.entries(ETO_METHODS).map(([key, method]) => (
        <button
          key={key}
          type="button"
          className={`method-tab ${activeMethod === key ? 'active' : ''}`}
          onClick={() => onChange(key)}
        >
          <span>{method.label}</span>
          <small>{method.description}</small>
        </button>
      ))}
    </nav>
  );
}

function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activeMethod, setActiveMethod] = useState('pm');

  async function load() {
    setLoading(true);
    setError('');
    try {
      const rows = await queryInflux();
      setData(rows);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  const latest = data[data.length - 1];
  const method = ETO_METHODS[activeMethod];
  const arm75Field = getField(activeMethod, 'arm', 75);
  const arm100Field = getField(activeMethod, 'arm', 100);
  const arm150Field = getField(activeMethod, 'arm', 150);
  const etr100Field = getField(activeMethod, 'etr', 100);
  const alt100Field = getField(activeMethod, 'alt', 100);
  const def100Field = getField(activeMethod, 'def', 100);
  const exc100Field = getField(activeMethod, 'exc', 100);
  const negAcum100Field = getField(activeMethod, 'neg_acum', 100);
  const etoField = getField(activeMethod, 'eto');

  const recommendations = useMemo(() => buildRecommendations(latest, activeMethod), [latest, activeMethod]);

  return (
    <main className="page">
      <header className="hero">
        <div>
          <p className="eyebrow">Monitoramento IoT do Vinhedo</p>
          <h1>Resumo do Balanço Hídrico Climatológico</h1>
        </div>
        <button onClick={load} disabled={loading}>
          <RefreshCw size={18} className={loading ? 'spin' : ''} /> Atualizar
        </button>
      </header>

      <MethodTabs activeMethod={activeMethod} onChange={setActiveMethod} />

      {error && <div className="error">{error}</div>}

      <section className="method-view">
        <section className="panel status-summary">
          <h2>Status do vinhedo por CAD — {method.label}</h2>
          <p>
            A CAD representa a capacidade máxima de armazenamento de água do solo. O ARM indica quanta água está armazenada no momento.
          </p>
          <div className="cad-status-grid">
            {CAD_CONFIGS.map(({ value }) => (
              <CadStatusCard
                key={value}
                cad={value}
                arm={latest?.[getField(activeMethod, 'arm', value)]}
                exc={latest?.[getField(activeMethod, 'exc', value)]}
                methodLabel={method.label}
                date={latest?.date}
              />
            ))}
          </div>
        </section>

        <section className="panel">
          <h2>Recomendações ao produtor</h2>
          <ul className="recommendations">
            {recommendations.map((rec, i) => <li key={i}>{rec}</li>)}
          </ul>
        </section>

        <section className="panel">
          <h2>Indicadores do Balanço Hídrico</h2>

          <table className="bhc-table">
            <thead>
              <tr>
                <th>CAD</th>
                <th>Status</th>
                <th>ARM (mm)</th>
                <th>ETR (mm)</th>
                <th>ALT (mm)</th>
                <th>DEF (mm)</th>
                <th>EXC (mm)</th>
                <th>NEG.ACUM (mm)</th>
              </tr>
            </thead>

            <tbody>
              {[75, 100, 150].map((cad) => {
                const arm = latest?.[`arm_cad${cad}_${activeMethod}`];
                const etr = latest?.[`etr_cad${cad}_${activeMethod}`];
                const alt = latest?.[`alt_cad${cad}_${activeMethod}`];
                const def = latest?.[`def_cad${cad}_${activeMethod}`];
                const exc = latest?.[`exc_cad${cad}_${activeMethod}`];
                const neg = latest?.[`neg_acum_cad${cad}_${activeMethod}`];

                const ratio = arm != null ? arm / cad : 0;

                let status = "Crítico";
                let statusClass = "status-critical";

                if (ratio >= 0.7) {
                  status = "Estável";
                  statusClass = "status-ok";
                } else if (ratio >= 0.4) {
                  status = "Alerta";
                  statusClass = "status-warning";
                }

                return (
                  <tr key={cad}>
                    <td>{cad}</td>

                    <td className={statusClass}>
                      {status}
                    </td>

                    <td>{arm?.toFixed(2) ?? "-"}</td>
                    <td>{etr?.toFixed(2) ?? "-"}</td>
                    <td>{alt?.toFixed(2) ?? "-"}</td>

                    <td className={def > 0 ? "danger-cell" : ""}>
                      {def?.toFixed(2) ?? "-"}
                    </td>

                    <td className={exc > 0 ? "excess-cell" : ""}>
                      {exc?.toFixed(2) ?? "-"}
                    </td>

                    <td>{neg?.toFixed(2) ?? "-"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </section>
      </section>

      <section className="charts">
        <div className="panel chart-panel">
          <h2>Evolução do ARM — {method.label}</h2>
          <ResponsiveContainer width="100%" height={320}>
            <LineChart data={data}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="date" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey={arm75Field} name="ARM CAD 75" dot={false} />
              <Line type="monotone" dataKey={arm100Field} name="ARM CAD 100" dot={false} />
              <Line type="monotone" dataKey={arm150Field} name="ARM CAD 150" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

    </main>
  );
}

createRoot(document.getElementById('root')).render(<App />);
