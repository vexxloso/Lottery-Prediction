import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { App as AntApp } from 'antd'
import 'antd/dist/reset.css'
import './index.css'
import App from './App.tsx'

const API_BASE = (import.meta.env.VITE_API_URL ?? 'http://localhost:8000').replace(/\/$/, '');
const originalFetch = window.fetch;
window.fetch = function (input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const url = typeof input === 'string' ? input : input instanceof Request ? input.url : String(input);
  const isApi = url.startsWith(API_BASE);
  const token = sessionStorage.getItem('platform_token');
  let initCopy = init;
  if (isApi && token) {
    const headers = new Headers(init?.headers);
    headers.set('Authorization', `Bearer ${token}`);
    initCopy = { ...init, headers };
  }
  return originalFetch.call(this, input, initCopy).then((res) => {
    if (isApi && res.status === 401) {
      sessionStorage.removeItem('platform_token');
      sessionStorage.removeItem('platform_auth');
      if (!url.includes('/api/auth/verify')) {
        window.location.href = '/login';
      }
    }
    return res;
  });
};

const theme = localStorage.getItem('theme') || 'light';
document.documentElement.setAttribute('data-theme', theme);

createRoot(document.getElementById('root')!).render(
    <BrowserRouter>
      <AntApp>
        <App />
      </AntApp>
    </BrowserRouter>,
)
