import { useEffect, useState, useRef } from 'react';
import { Link } from 'react-router-dom';
import FullCalendar from '@fullcalendar/react';
import dayGridPlugin from '@fullcalendar/daygrid';
import type { EventContentArg } from '@fullcalendar/core';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from 'recharts';
import { LOTTERIES } from '../mock/data';
import { useApuestasSeries } from './resultados/useApuestasSeries';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

interface NextDrawItem {
  lottery: string;
  last_draw_date?: string;
  next_draw_date?: string;
  next_funds_prediction?: {
    bote_stats?: {
      median?: number;
    };
    premios_stats?: {
      median?: number;
    };
  };
}

function getNextDrawForChart(
  items: NextDrawItem[],
  lottery: string,
): NextDrawPrediction | undefined {
  const m = items.find((it) => it.lottery === lottery);
  if (!m?.next_draw_date) return undefined;
  return {
    date: m.next_draw_date,
    premios: m.next_funds_prediction?.premios_stats?.median,
    bote: m.next_funds_prediction?.bote_stats?.median,
  };
}

interface CalendarEvent {
  id: string;
  title: string;
  start: string;
  allDay: boolean;
  extendedProps: {
    lottery: string;
    imageUrl: string;
  };
}

function getLotteryImage(lottery: string): string {
  if (lottery === 'euromillones') return '/images/euromillones.png';
  if (lottery === 'el-gordo') return '/images/el-gordo.png';
  if (lottery === 'la-primitiva') return '/images/la-primitiva.png';
  return '/images/euromillones.png';
}

function renderEventContent(arg: EventContentArg) {
  const lottery = (arg.event.extendedProps.lottery as string) || arg.event.title;
  const imageUrl = arg.event.extendedProps.imageUrl as string | undefined;
  return (
    <div
      style={{
        position: 'relative',
        width: '100%',
        height: '100%',
        boxSizing: 'border-box',
      }}
      aria-label={lottery}
    >
      {imageUrl && (
        <img
          src={imageUrl}
          alt={lottery}
          style={{
            position: 'absolute',
            left: 4,
            top: 4,
            width: 32,
            height: 32,
            objectFit: 'contain',
          }}
        />
      )}
    </div>
  );
}

interface NextDrawPrediction {
  date: string;
  premios?: number;
  bote?: number;
}

function PremiosBoteChart({
  lottery,
  title,
  nextDraw,
}: {
  lottery: 'euromillones' | 'la-primitiva' | 'el-gordo';
  title: string;
  nextDraw?: NextDrawPrediction | null;
}) {
  const { points, loading, error } = useApuestasSeries(lottery, '2m');

  const chartData =
    nextDraw?.date && (nextDraw.premios != null || nextDraw.bote != null)
      ? [
          ...points,
          {
            date: nextDraw.date,
            premios: nextDraw.premios ?? null,
            premio_bote: nextDraw.bote ?? null,
            isNext: true,
          },
        ]
      : points;

  const maxPremios = chartData.reduce(
    (max, p) => (p.premios != null && p.premios > max ? p.premios : max),
    0,
  );
  const maxBote = chartData.reduce(
    (max, p) => (p.premio_bote != null && p.premio_bote > max ? p.premio_bote : max),
    0,
  );
  const dataWithPct = chartData.map((p, i) => {
    const premios_pct =
      maxPremios > 0 && p.premios != null ? (p.premios / maxPremios) * 100 : null;
    const premio_bote_pct =
      maxBote > 0 && p.premio_bote != null ? (p.premio_bote / maxBote) * 100 : null;
    const isLastPoint = chartData.length >= 1 && i === chartData.length - 1;
    const isLastTwo = chartData.length >= 2 && i >= chartData.length - 2;
    return {
      ...p,
      premios_pct: isLastPoint ? null : premios_pct,
      premio_bote_pct: isLastPoint ? null : premio_bote_pct,
      premios_pct_dashed: isLastTwo ? premios_pct : null,
      premio_bote_pct_dashed: isLastTwo ? premio_bote_pct : null,
    };
  });

  const formatEuro = (value?: number | null) => {
    if (value == null) return '-';
    try {
      return `${new Intl.NumberFormat('es-ES', {
        maximumFractionDigits: value >= 1_000_000 ? 1 : 0,
      }).format(value)} €`;
    } catch {
      return `${value.toFixed(0)} €`;
    }
  };

  if (loading && points.length === 0) {
    return (
      <div className="dashboard-chart-wrap">
        <h3 className="dashboard-chart-title">{title}</h3>
        <p style={{ color: 'var(--color-text-muted)', fontSize: '0.875rem' }}>Cargando…</p>
      </div>
    );
  }
  if (error) {
    return (
      <div className="dashboard-chart-wrap">
        <h3 className="dashboard-chart-title">{title}</h3>
        <p style={{ color: 'var(--color-error)', fontSize: '0.875rem' }}>{error}</p>
      </div>
    );
  }
  if (points.length === 0) {
    return (
      <div className="dashboard-chart-wrap">
        <h3 className="dashboard-chart-title">{title}</h3>
        <p style={{ color: 'var(--color-text-muted)', fontSize: '0.875rem' }}>Sin datos (2 meses)</p>
      </div>
    );
  }

  const renderDot =
    (stroke: string) =>
    (props: { cx?: number; cy?: number; payload?: { isNext?: boolean } }) => {
      const { cx = 0, cy = 0, payload } = props;
      if (payload?.isNext) {
        return (
          <circle
            cx={cx}
            cy={cy}
            r={5}
            fill={stroke}
            stroke="var(--color-surface)"
            strokeWidth={2}
          />
        );
      }
      return <circle cx={cx} cy={cy} r={0} fill="transparent" />;
    };

  return (
    <div className="dashboard-chart-wrap">
      <h3 className="dashboard-chart-title">{title}</h3>
      <div style={{ width: '100%', height: 220 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={dataWithPct} margin={{ top: 8, right: 12, left: 0, bottom: 16 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" tick={{ fontSize: 10 }} tickMargin={6} minTickGap={24} />
            <YAxis
              tick={{ fontSize: 10 }}
              domain={[0, 100]}
              tickFormatter={(v: number) => `${v.toFixed(0)}%`}
            />
            <Tooltip
              formatter={(value, name, props) => {
                const v = value as number | null;
                const raw = name === 'Premios' ? props?.payload?.premios : props?.payload?.premio_bote;
                const pct = v != null ? `${Number(v).toFixed(1)}%` : '-';
                return [`${pct} · ${formatEuro(raw)}`, name];
              }}
              labelFormatter={(label: string, payload?: { payload?: { isNext?: boolean } }[]) =>
                payload?.[0]?.payload?.isNext ? `Próx. draw (${label})` : `Fecha: ${label}`}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="premios_pct"
              name="Premios"
              stroke="var(--color-primary, #2563eb)"
              strokeWidth={1}
              dot={renderDot('var(--color-primary, #2563eb)')}
            />
            <Line
              type="monotone"
              dataKey="premios_pct_dashed"
              stroke="var(--color-primary, #2563eb)"
              strokeWidth={1}
              strokeDasharray="5 5"
              dot={false}
              legendType="none"
              connectNulls={false}
            />
            <Line
              type="monotone"
              dataKey="premio_bote_pct"
              name="Bote"
              stroke="var(--color-accent, #16a34a)"
              strokeWidth={1}
              dot={renderDot('var(--color-accent, #16a34a)')}
            />
            <Line
              type="monotone"
              dataKey="premio_bote_pct_dashed"
              stroke="var(--color-accent, #16a34a)"
              strokeWidth={1}
              strokeDasharray="5 5"
              dot={false}
              legendType="none"
              connectNulls={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

interface SampleTicketsLottery {
  last_draw_date: string | null;
  tickets: Array<
    | { mains: number[]; stars?: number[] }
    | { mains: number[]; reintegro?: number }
    | { mains: number[]; clave?: number }
  >;
}

export function Dashboard() {
  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [metaItems, setMetaItems] = useState<NextDrawItem[]>([]);
  const [sampleTickets, setSampleTickets] = useState<{
    euromillones: SampleTicketsLottery;
    'la-primitiva': SampleTicketsLottery;
    'el-gordo': SampleTicketsLottery;
  } | null>(null);
  const [sampleTicketsIndex, setSampleTicketsIndex] = useState(0);
  const sampleIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const formatEuro = (value?: number) => {
    if (value == null) return '-';
    try {
      return `${new Intl.NumberFormat('es-ES', {
        maximumFractionDigits: value >= 1_000_000 ? 1 : 0,
      }).format(value)} €`;
    } catch {
      return `${value.toFixed(0)} €`;
    }
  };

  useEffect(() => {
    const loadNextDraws = async () => {
      try {
        const res = await fetch(`${API_URL}/api/metadata/next-draws`);
        const data = await res.json();
        if (!res.ok) {
          setEvents([]);
          return;
        }
        const items = (data.items ?? []) as NextDrawItem[];
        setMetaItems(items);
        const mapped: CalendarEvent[] = items
          .filter((it) => it.next_draw_date)
          .map((it) => {
            const slug = it.lottery;
            const config = LOTTERIES.find((l) => l.id === slug);
            const title = config?.name ?? slug;
            const imageUrl = getLotteryImage(slug);
            return {
              id: slug,
              title,
              start: it.next_draw_date as string,
              allDay: true,
              extendedProps: {
                lottery: title,
                imageUrl,
              },
            };
          });
        setEvents(mapped);
      } catch {
        setEvents([]);
      }
    };
    loadNextDraws();
  }, []);

  useEffect(() => {
    const loadSampleTickets = async () => {
      try {
        const res = await fetch(`${API_URL}/api/dashboard/sample-tickets?count=10`);
        const data = await res.json();
        if (!res.ok) return;
        setSampleTickets({
          euromillones: data.euromillones ?? { last_draw_date: null, tickets: [] },
          'la-primitiva': data['la-primitiva'] ?? { last_draw_date: null, tickets: [] },
          'el-gordo': data['el-gordo'] ?? { last_draw_date: null, tickets: [] },
        });
      } catch {
        setSampleTickets(null);
      }
    };
    loadSampleTickets();
  }, []);

  useEffect(() => {
    if (!sampleTickets) return;
    const hasAny =
      (sampleTickets.euromillones.tickets?.length ?? 0) > 0 ||
      (sampleTickets['la-primitiva'].tickets?.length ?? 0) > 0 ||
      (sampleTickets['el-gordo'].tickets?.length ?? 0) > 0;
    if (!hasAny) return;
    sampleIntervalRef.current = setInterval(() => {
      setSampleTicketsIndex((i) => (i + 1) % 10);
    }, 4000);
    return () => {
      if (sampleIntervalRef.current) {
        clearInterval(sampleIntervalRef.current);
        sampleIntervalRef.current = null;
      }
    };
  }, [sampleTickets]);

  return (
    <>
      <section className="dashboard-lottery-cards" aria-label="Próximo bote, premios y muestra del pool">
        {(['euromillones', 'la-primitiva', 'el-gordo'] as const).map((slug) => {
          const m = metaItems.find((it) => it.lottery === slug);
          const boteMedian = m?.next_funds_prediction?.bote_stats?.median;
          const premiosMedian = m?.next_funds_prediction?.premios_stats?.median;
          const label =
            slug === 'euromillones'
              ? 'Euromillones'
              : slug === 'la-primitiva'
                ? 'La Primitiva'
                : 'El Gordo';
          const imgSrc =
            slug === 'euromillones'
              ? '/images/euromillones.png'
              : slug === 'la-primitiva'
                ? '/images/la-primitiva.png'
                : '/images/el-gordo.png';
          const data = sampleTickets?.[slug];
          const tickets = data?.tickets ?? [];
          const idx = tickets.length > 0 ? sampleTicketsIndex % tickets.length : 0;
          const ticket = tickets[idx];
          const bettingHref =
            slug === 'el-gordo'
              ? '/resultados/el-gordo?tab=betting'
              : `/resultados/${slug}?tab=apuestas`;
          return (
            <Link
              key={slug}
              to={bettingHref}
              className="dashboard-lottery-card"
              aria-label={`${label}, ir a apuestas`}
            >
              <div className="dashboard-lottery-card-top">
                <div className="dashboard-lottery-card-header">
                <img src={imgSrc} alt="" className="dashboard-lottery-card-img" aria-hidden />
                <div className="dashboard-lottery-card-heading">
                  <h3 className="dashboard-lottery-card-title">{label}</h3>
                  <p className="dashboard-lottery-card-row">
                    Próx. bote: {formatEuro(boteMedian)}
                  </p>
                  <p className="dashboard-lottery-card-row">
                    Premios: {formatEuro(premiosMedian)}
                  </p>
                </div>
              </div>
                {m?.next_draw_date ? (
                  <div className="dashboard-lottery-card-date-top" aria-label="Próximo sorteo">
                    {m.next_draw_date}
                  </div>
                ) : null}
              </div>
              <div className="dashboard-lottery-card-sample">
                <div className="dashboard-sample-ticket-balls" key={`${slug}-${idx}`}>
                  {ticket ? (
                    <>
                      {(ticket.mains ?? []).map((n, i) => (
                        <span key={`m-${i}`} className="dashboard-sample-ticket-ball">
                          {String(n).padStart(2, '0')}
                        </span>
                      ))}
                      {'stars' in ticket && (ticket.stars ?? []).length > 0 && (
                        <>
                          {(ticket.stars ?? []).map((s, i) => (
                            <span key={`s-${i}`} className="dashboard-sample-ticket-ball star">
                              {String(s).padStart(2, '0')}
                            </span>
                          ))}
                        </>
                      )}
                      {'clave' in ticket && ticket.clave != null && (
                        <span className="dashboard-sample-ticket-ball star">
                          {String(ticket.clave).padStart(2, '0')}
                        </span>
                      )}
                      {'reintegro' in ticket && ticket.reintegro != null && (
                        <span className="dashboard-sample-ticket-ball star">
                          R {ticket.reintegro}
                        </span>
                      )}
                    </>
                  ) : (
                    <span className="dashboard-lottery-card-no-pool">Sin pool</span>
                  )}
                </div>
              </div>
            </Link>
          );
        })}
      </section>

      <section className="dashboard-graph-section" aria-label="Premios y Bote últimos 2 meses">
        <h2 className="dashboard-graph-heading">Premios y Bote (últimos 2 meses)</h2>
        <div className="dashboard-chart-grid">
          <PremiosBoteChart
            lottery="euromillones"
            title="Euromillones"
            nextDraw={getNextDrawForChart(metaItems, 'euromillones')}
          />
          <PremiosBoteChart
            lottery="la-primitiva"
            title="La Primitiva"
            nextDraw={getNextDrawForChart(metaItems, 'la-primitiva')}
          />
          <PremiosBoteChart
            lottery="el-gordo"
            title="El Gordo"
            nextDraw={getNextDrawForChart(metaItems, 'el-gordo')}
          />
        </div>
      </section>

      <div className="calendar-wrap">
        <FullCalendar
          plugins={[dayGridPlugin]}
          initialView="dayGridMonth"
          events={events}
          eventContent={renderEventContent}
          eventDisplay="block"
          headerToolbar={{ left: 'prev,next', center: 'title', right: '' }}
          height="auto"
        />
      </div>
    </>
  );
}
