import { useEffect, useState } from 'react';
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
      return null;
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
              formatter={(value: number | null, name: string, props: { payload?: { premios?: number | null; premio_bote?: number | null } }) => {
                const raw = name === 'Premios' ? props?.payload?.premios : props?.payload?.premio_bote;
                const pct = value != null ? `${value.toFixed(1)}%` : '-';
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

export function Dashboard() {
  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [metaItems, setMetaItems] = useState<NextDrawItem[]>([]);

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

  return (
    <>
      <div
        style={{
          marginBottom: 'var(--space-sm)',
          display: 'flex',
          justifyContent: 'flex-end',
          gap: '1.25rem',
          fontSize: '0.75rem',
        }}
      >
        {['euromillones', 'la-primitiva', 'el-gordo'].map((slug) => {
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
          return (
            <div key={slug} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <img
                src={imgSrc}
                alt={label}
                style={{ width: 16, height: 16, objectFit: 'contain' }}
              />
              <div
                style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start' }}
              >
                <span>{label}</span>
                <span style={{ color: '#6b7280', fontSize: '0.7rem' }}>
                  Próx. bote: {formatEuro(boteMedian)}
                </span>
                <span style={{ color: '#9ca3af', fontSize: '0.68rem' }}>
                  Premios: {formatEuro(premiosMedian)}
                </span>
              </div>
            </div>
          );
        })}
      </div>

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
