import { Navigate, useLocation } from 'react-router-dom';

const AUTH_KEY = 'platform_auth';

function getIsAuthenticated(): boolean {
  return sessionStorage.getItem(AUTH_KEY) === '1';
}

export function RequireAuth({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  if (!getIsAuthenticated()) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }
  return <>{children}</>;
}
