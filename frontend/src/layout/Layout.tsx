import { useState } from 'react';
import { Link, NavLink, Outlet, useNavigate } from 'react-router-dom';
import { LogoutOutlined, MenuOutlined, MoonOutlined, UpOutlined, UserOutlined } from '@ant-design/icons';
import { useTheme } from '../context/ThemeContext';

const AUTH_KEY = 'platform_auth';

const navItems = [
  { to: '/resultados/la-primitiva', label: 'La Primitiva', icon: '/images/la-primitiva.png' },
  { to: '/resultados/euromillones', label: 'Euromillones', icon: '/images/euromillones.png' },
  { to: '/resultados/el-gordo', label: 'El Gordo', icon: '/images/el-gordo.png' },
];
const navRightItems = [
  { to: '/bot-cuentas', label: 'Cuentas bot', icon: null },
];

export function Layout() {
  const navigate = useNavigate();
  const [menuOpen, setMenuOpen] = useState(false);
  const { theme, toggleTheme } = useTheme();

  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  };

  const handleLogout = () => {
    setMenuOpen(false);
    sessionStorage.removeItem(AUTH_KEY);
    sessionStorage.removeItem('platform_token');
    navigate('/login', { replace: true });
  };

  return (
    <div className="app">
      <header className="app-header">
        <Link to="/" className="app-logo">
          <img src="/images/logo_loterias.svg" alt="Predicción Lotería" className="app-logo-img" />
        </Link>
        <nav className={`app-nav ${menuOpen ? 'open' : ''}`} aria-label="Navegación principal">
          {navItems.map(({ to, label, icon }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}
              end={to === '/'}
              onClick={() => setMenuOpen(false)}
              title={label}
              aria-label={label}
            >
              <img src={icon} alt="" className="nav-link-icon" aria-hidden />
            </NavLink>
          ))}
        </nav>
        <div className="app-header-right">
          <nav className="app-nav app-nav--right" aria-label="Cuentas y configuración">
            {navRightItems.map(({ to, label }) => (
              <NavLink
                key={to}
                to={to}
                className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}
                onClick={() => setMenuOpen(false)}
                title={label}
                aria-label={label}
              >
                <UserOutlined className="nav-link-icon nav-link-icon--user" />
              </NavLink>
            ))}
            <button
              type="button"
              className="nav-link nav-link--logout"
              onClick={handleLogout}
              title="Cerrar sesión"
              aria-label="Cerrar sesión"
            >
              <LogoutOutlined className="nav-link-icon nav-link-icon--logout" />
            </button>
          </nav>
          <button
            type="button"
            className="menu-toggle"
            aria-label="Abrir o cerrar menú"
            aria-expanded={menuOpen}
            onClick={() => setMenuOpen((prev) => !prev)}
          >
            <MenuOutlined className="menu-toggle-icon" />
          </button>
        </div>
      </header>
      <main className="app-main">
        <Outlet />
      </main>
      <div className="fab-group" aria-label="Acciones rápidas">
        <button
          type="button"
          className="fab fab--top"
          onClick={scrollToTop}
          title="Subir"
          aria-label="Subir al inicio"
        >
          <UpOutlined className="fab-icon" />
        </button>
        <button
          type="button"
          className="fab fab--theme"
          onClick={toggleTheme}
          title={theme === 'light' ? 'Modo oscuro' : 'Modo claro'}
          aria-label={theme === 'light' ? 'Cambiar a modo oscuro' : 'Cambiar a modo claro'}
        >
          {theme === 'light' ? <MoonOutlined className="fab-icon" /> : <span className="fab-icon">☀️</span>}
        </button>
      </div>
    </div>
  );
}
