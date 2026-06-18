import React, { useState, useEffect } from 'react';

function App() {
  const [conectado, setConectado] = useState(false);
  const [dadosCurtos, setDadosCurtos] = useState({
    temperatura_media: 0,
    temperatura_minima: 0,
    temperatura_maxima: 0,
    umidade_media: 0
  });
  const [dadosBhc, setDadosBhc] = useState(null);

  useEffect(() => {
    // Detecta o host dinamicamente (útil para rodar local ou em redes Docker)
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}`;
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => setConectado(true);
    ws.onclose = () => setConectado(false);
    
    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      
      // Roteamento de estados baseado na assinatura do payload do Flink
      if (payload.tipo_registro === "SHORT_TERM_STATS") {
        setDadosCurtos(payload);
      } else if (payload.tipo_registro === "DAILY_BHC_COMPLETO") {
        setDadosBhc(payload);
      }
    };

    return () => ws.close();
  }, []);

  // Função auxiliar para renderizar as Badges de Alerta dinamicamente com Tailwind
  const obterBadgeAlerta = (classe) => {
    const estilos = {
      'CRÍTICO': 'bg-red-100 text-red-800 border-red-200',
      'ATENÇÃO': 'bg-amber-100 text-amber-800 border-amber-200',
      'ESTÁVEL': 'bg-green-100 text-green-800 border-green-200'
    };
    return estilos[classe] || 'bg-gray-100 text-gray-800';
  };

  return (
    <div className="min-h-screen bg-slate-50 p-6 text-slate-800">
      {/* Header */}
      <header className="mb-8 flex flex-col justify-between sm:flex-row sm:items-center border-b border-slate-200 pb-5">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-slate-900 flex items-center gap-2">
            🍇 Painel de Monitoramento Hídrico
          </h1>
          <p className="text-sm text-slate-500 mt-1">Vinhedos Inteligentes • Dados Computados via Apache Flink</p>
        </div>
        <div className="mt-4 sm:mt-0 flex items-center gap-2">
          <span className={`h-3 w-3 rounded-full ${conectado ? 'bg-emerald-500 animate-pulse' : 'bg-rose-500'}`}></span>
          <span className="text-sm font-medium text-slate-600">
            {conectado ? 'Esteira de dados ativa' : 'Desconectado do cluster'}
          </span>
        </div>
      </header>

      {/* Grid Principal */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* Card 1: Tempo Real (Job 1) */}
        <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200/80">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-bold text-slate-900">⚡ Condições de Campo</h2>
            <span className="text-xs bg-slate-100 px-2 py-1 rounded text-slate-500 font-mono">Job 1 (30s)</span>
          </div>
          <div className="space-y-4">
            <div>
              <p className="text-sm text-slate-500 uppercase font-semibold tracking-wider">Temperatura Média</p>
              <p className="text-4xl font-extrabold text-slate-900 mt-1">
                {dadosCurtos.temperatura_media ? dadosCurtos.temperatura_media.toFixed(1) : '--'}<span className="text-xl font-normal text-slate-400"> °C</span>
              </p>
              <div className="flex gap-4 mt-1 text-xs text-slate-500">
                <span>Mín: {dadosCurtos.temperatura_minima !== opacity ? dadosCurtos.temperatura_minima.toFixed(1) : '--'}°C</span>
                <span>Máx: {dadosCurtos.temperatura_maxima !== -opacity ? dadosCurtos.temperatura_maxima.toFixed(1) : '--'}°C</span>
              </div>
            </div>
            <div className="pt-4 border-t border-slate-100">
              <p className="text-sm text-slate-500 uppercase font-semibold tracking-wider">Umidade Média do Ar</p>
              <p className="text-4xl font-extrabold text-slate-900 mt-1">
                {dadosCurtos.umidade_media ? dadosCurtos.umidade_media.toFixed(1) : '--'}<span className="text-xl font-normal text-slate-400"> %</span>
              </p>
            </div>
          </div>
        </div>

        {/* Card 2 & 3: Balanço Hídrico Completo (Job 3) */}
        <div className="lg:col-span-2 bg-white p-6 rounded-xl shadow-sm border border-slate-200/80">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-lg font-bold text-slate-900">📅 Balanço Hídrico Climatológico (Clima/Solo)</h2>
            <span className="text-xs bg-purple-100 text-purple-700 px-2 py-1 rounded font-mono font-semibold">Job 3 (Thornthwaite)</span>
          </div>

          {dadosBhc ? (
            <div>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-6 bg-slate-50 p-4 rounded-lg">
                <div>
                  <p className="text-xs text-slate-400 font-semibold uppercase">Precipitação Diária</p>
                  <p className="text-xl font-bold text-slate-800">{dadosBhc.precipitacao_diaria.toFixed(1)} mm</p>
                </div>
                <div>
                  <p className="text-xs text-slate-400 font-semibold uppercase">ETo Aplicada (FAO-56)</p>
                  <p className="text-xl font-bold text-slate-800">{dadosBhc.eto_aplicada.toFixed(1)} mm</p>
                </div>
                <div className="col-span-2 md:col-span-1">
                  <p className="text-xs text-slate-400 font-semibold uppercase">Balanço Líquido</p>
                  <p className={`text-xl font-bold ${(dadosBhc.precipitacao_diaria - dadosBhc.eto_aplicada) >= 0 ? 'text-emerald-600' : 'text-rose-600'}`}>
                    {(dadosBhc.precipitacao_diaria - dadosBhc.eto_aplicada).toFixed(1)} mm
                  </p>
                </div>
              </div>

              {/* Exibição dos Diferentes Cenários de Solo (CAD Múltiplas) */}
              <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-3">Análise Dinâmica por Capacidade de Água Disponível (CAD)</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(dadosBhc.cenarios_cad).map(([chaveCad, dados]) => (
                  <div key={chaveCad} className="p-4 border border-slate-100 rounded-lg bg-white shadow-xs">
                    <div className="flex justify-between items-start mb-2">
                      <h4 className="font-bold text-slate-700 uppercase text-xs tracking-wide">{chaveCad.replace('_', ' ')}</h4>
                      <span className={`text-[10px] uppercase px-2 py-0.5 rounded font-bold border ${obterBadgeAlerta(dados.classe_alerta_videira)}`}>
                        {dados.classe_alerta_videira}
                      </span>
                    </div>
                    <div className="space-y-1.5 text-sm">
                      <div className="flex justify-between">
                        <span className="text-slate-500">Armazenamento (ARM):</span>
                        <span className="font-semibold text-slate-900">{dados.armazenamento.toFixed(1)} mm</span>
                      </div>
                      <div className="w-full bg-slate-100 h-2 rounded-full overflow-hidden">
                        <div 
                          className={`h-full ${dados.percentual_disponivel < 40 ? 'bg-rose-500' : dados.percentual_disponivel < 70 ? 'bg-amber-500' : 'bg-emerald-500'}`}
                          style={{ width: `${Math.min(100, dados.percentual_disponivel)}%` }}
                        ></div>
                      </div>
                      <div className="flex justify-between text-xs text-slate-400 pt-1">
                        <span>Água Disponível: {dados.percentual_disponivel.toFixed(1)}%</span>
                      </div>
                      <div className="grid grid-cols-3 gap-1 pt-2 border-t border-slate-50 text-[11px] text-slate-500">
                        <div><span className="block text-slate-400 text-[9px] uppercase font-bold">ETR</span> {dados.evapotranspiracao_real.toFixed(1)}mm</div>
                        <div><span className="block text-slate-400 text-[9px] uppercase font-bold">Déficit</span> {dados.deficit.toFixed(1)}mm</div>
                        <div><span className="block text-slate-400 text-[9px] uppercase font-bold">Excesso</span> {dados.excesso.toFixed(1)}mm</div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-48 text-slate-400 border-2 border-dashed border-slate-200 rounded-lg">
              <span className="text-2xl animate-spin mb-2">⏳</span>
              <p className="text-sm">Aguardando fechamento do ciclo diário no Flink para renderizar o BHC...</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;