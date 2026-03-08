import { Routes, Route } from 'react-router-dom';
import { Layout } from './layout/Layout';
import { Dashboard } from './pages/Dashboard';
import { ResultadosLaPrimitiva, ResultadosEuromillones, ResultadosElGordo } from './pages/resultados';
import { SimulationPage } from './pages/SimulationPage';
import { BotCredentialsPage } from './pages/BotCredentialsPage';

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="resultados/la-primitiva" element={<ResultadosLaPrimitiva />} />
        <Route path="resultados/euromillones" element={<ResultadosEuromillones />} />
        <Route path="resultados/el-gordo" element={<ResultadosElGordo />} />
        <Route path="simulacion/:lottery/:drawId" element={<SimulationPage />} />
        <Route path="bot-cuentas" element={<BotCredentialsPage />} />
      </Route>
    </Routes>
  );
}

export default App;
