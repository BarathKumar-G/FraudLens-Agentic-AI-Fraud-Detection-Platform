import { useState, useEffect, useRef } from 'react';
import { AlertCircle, ShieldAlert, ShieldCheck, Activity, Search, Clock } from 'lucide-react';

const API_URL = "http://98.92.3.19:8000";

export default function App() {
  const [data, setData] = useState({ transactions: [] });
  const [alerts, setAlerts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedTx, setSelectedTx] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [newTxIds, setNewTxIds] = useState(new Set());
  const prevTxIdsRef = useRef(new Set());

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [txRes, metricsRes, alertsRes] = await Promise.all([
          fetch(`${API_URL}/api/transactions`),
          fetch(`${API_URL}/api/metrics`),
          fetch(`${API_URL}/api/alerts`)
        ]);
        if (!txRes.ok || !metricsRes.ok || !alertsRes.ok) throw new Error("API failed");
        
        const transactions = await txRes.json();
        const fetchedAlerts = await alertsRes.json();
        
        const currentIds = new Set(transactions.map(t => t.transaction_id));
        const prevIds = prevTxIdsRef.current;
        
        if (prevIds.size > 0) {
          const newIds = new Set([...currentIds].filter(id => !prevIds.has(id)));
          if (newIds.size > 0) {
            setNewTxIds(newIds);
            setTimeout(() => {
              setNewTxIds(new Set());
            }, 2000);
          }
        }
        
        prevTxIdsRef.current = currentIds;
        
        setData({ transactions });
        setAlerts(fetchedAlerts);
        setLastUpdated(new Date());
        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  const getRiskColor = (tier) => {
    switch (tier?.toLowerCase()) {
      case 'critical': return 'text-red-600 bg-red-100 border-red-200';
      case 'high': return 'text-orange-600 bg-orange-100 border-orange-200';
      case 'medium': return 'text-yellow-600 bg-yellow-100 border-yellow-200';
      case 'low': return 'text-green-600 bg-green-100 border-green-200';
      default: return 'text-gray-600 bg-gray-100 border-gray-200';
    }
  };

  const sortedAlerts = [...alerts].sort((a, b) => {
    if (a.risk_tier?.toLowerCase() === 'critical' && b.risk_tier?.toLowerCase() !== 'critical') return -1;
    if (a.risk_tier?.toLowerCase() !== 'critical' && b.risk_tier?.toLowerCase() === 'critical') return 1;
    return 0;
  });

  const totalTransactions = data.transactions.length;
  const fraudTxs = data.transactions.filter(t => t.fraud_probability >= 0.5).length;
  const fraudRate = totalTransactions > 0 ? ((fraudTxs / totalTransactions) * 100).toFixed(1) : 0;
  
  const rulesCounts = {
    critical: data.transactions.filter(t => t.risk_tier?.toLowerCase() === 'critical').length,
    high: data.transactions.filter(t => t.risk_tier?.toLowerCase() === 'high').length,
    medium: data.transactions.filter(t => t.risk_tier?.toLowerCase() === 'medium').length,
    low: data.transactions.filter(t => t.risk_tier?.toLowerCase() === 'low').length,
  };

  const actionCounts = {
    block: data.transactions.filter(t => t.agent_action?.toLowerCase() === 'block').length,
    flag: data.transactions.filter(t => t.agent_action?.toLowerCase() === 'flag').length,
    monitor: data.transactions.filter(t => t.agent_action?.toLowerCase() === 'monitor').length,
    allow: data.transactions.filter(t => t.agent_action?.toLowerCase() === 'allow').length,
  };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 font-sans p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        <header className="flex justify-between items-center pb-4 border-b border-gray-200">
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Activity className="text-blue-600" />
            FraudLens Dashboard
          </h1>
          <div className="flex items-center gap-4 text-sm font-medium">
            {lastUpdated && (
              <span className="flex items-center gap-1 text-gray-500">
                <Clock className="w-4 h-4" />
                Last Updated: {lastUpdated.toLocaleTimeString()}
              </span>
            )}
            {loading && <span className="text-blue-500 animate-pulse border px-2 py-1 rounded bg-blue-50">Initializing...</span>}
            {!loading && !error && <span className="text-green-600 flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></div> Live</span>}
            {error && !loading && <span className="text-red-500 bg-red-50 px-2 py-1 rounded border border-red-200">API Error</span>}
          </div>
        </header>

        {loading && data.transactions.length === 0 ? (
           <div className="flex justify-center items-center h-64 text-gray-500 flex-col gap-4">
             <div className="w-8 h-8 rounded-full border-4 border-blue-200 border-t-blue-600 animate-spin"></div>
             <div>Loading Initial Real-time Data...</div>
           </div>
        ) : (
          <>
            {error && !data.transactions.length && (
              <div className="p-4 bg-red-50 text-red-700 rounded-lg border border-red-200 flex items-center gap-2">
                <AlertCircle />
                Failed to connect to API ({API_URL}). Backend might not be running.
              </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
              <div className="col-span-1 lg:col-span-3 space-y-6">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                  <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100">
                    <div className="text-sm text-gray-500 mb-1">Total Transactions</div>
                    <div className="text-2xl font-semibold">{totalTransactions}</div>
                  </div>
                  <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100">
                    <div className="text-sm text-gray-500 mb-1">Fraud Rate</div>
                    <div className="text-2xl font-semibold">{fraudRate}%</div>
                  </div>
                  
                  <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100">
                    <div className="text-sm text-gray-500 mb-2">Risk Tiers</div>
                    <div className="flex gap-2 text-xs font-semibold">
                      <div className="flex flex-col text-red-600"><span>CRT</span><span>{rulesCounts.critical}</span></div>
                      <div className="flex flex-col text-orange-600"><span>HGH</span><span>{rulesCounts.high}</span></div>
                      <div className="flex flex-col text-yellow-600"><span>MED</span><span>{rulesCounts.medium}</span></div>
                      <div className="flex flex-col text-green-600"><span>LOW</span><span>{rulesCounts.low}</span></div>
                    </div>
                  </div>

                  <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100">
                    <div className="text-sm text-gray-500 mb-2">Actions</div>
                    <div className="flex gap-2 text-xs font-semibold">
                      <div className="flex flex-col text-red-600"><span>BLK</span><span>{actionCounts.block}</span></div>
                      <div className="flex flex-col text-orange-600"><span>FLG</span><span>{actionCounts.flag}</span></div>
                      <div className="flex flex-col text-blue-600"><span>MON</span><span>{actionCounts.monitor}</span></div>
                      <div className="flex flex-col text-green-600"><span>ALW</span><span>{actionCounts.allow}</span></div>
                    </div>
                  </div>
                </div>

                <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
                  <div className="p-4 border-b border-gray-100">
                    <h2 className="text-lg font-semibold">Recent Transactions</h2>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm text-left">
                      <thead className="bg-gray-50 text-gray-500 uppercase text-xs font-semibold">
                        <tr>
                          <th className="px-4 py-3">ID</th>
                          <th className="px-4 py-3">Amount</th>
                          <th className="px-4 py-3">Risk Tier</th>
                          <th className="px-4 py-3">Probability</th>
                          <th className="px-4 py-3">Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {data.transactions.length === 0 && !loading && (
                          <tr>
                            <td colSpan="5" className="px-4 py-8 text-center text-gray-500">
                              No transactions found
                            </td>
                          </tr>
                        )}
                        {data.transactions.map((t) => {
                          const isNew = newTxIds.has(t.transaction_id);
                          return (
                          <tr 
                            key={t.transaction_id} 
                            onClick={() => setSelectedTx(t)}
                            className={`border-b border-gray-50 hover:bg-gray-50 cursor-pointer transition-all duration-500
                              ${t.risk_tier?.toLowerCase() === 'critical' ? 'bg-red-50/30' : ''}
                              ${t.risk_tier?.toLowerCase() === 'high' ? 'bg-orange-50/30' : ''}
                              ${t.risk_tier?.toLowerCase() === 'medium' ? 'bg-yellow-50/30' : ''}
                              ${t.risk_tier?.toLowerCase() === 'low' ? 'bg-green-50/30' : ''}
                              ${isNew ? 'bg-blue-100 ring-2 ring-inset ring-blue-400 font-bold scale-[1.01] shadow' : ''}
                            `}
                          >
                            <td className="px-4 py-3 font-mono text-xs">{t.transaction_id}</td>
                            <td className="px-4 py-3 font-medium">${t.amount?.toFixed(2)}</td>
                            <td className="px-4 py-3">
                              <span className={`px-2 py-1 rounded-full text-xs font-medium border ${getRiskColor(t.risk_tier)}`}>
                                {t.risk_tier?.toUpperCase()}
                              </span>
                            </td>
                            <td className="px-4 py-3">{(t.fraud_probability * 100).toFixed(1)}%</td>
                            <td className="px-4 py-3 capitalize font-medium">{t.agent_action}</td>
                          </tr>
                        )})}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              <div className="col-span-1 space-y-6">
                <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden h-full flex flex-col">
                  <div className="p-4 border-b border-gray-100 bg-red-50/50 flex-shrink-0">
                    <h2 className="text-lg font-semibold flex items-center gap-2 text-red-700">
                      <ShieldAlert className="text-red-500 animate-pulse" />
                      Live Alerts Panel
                    </h2>
                  </div>
                  <div className="p-4 space-y-3 flex-1 overflow-y-auto min-h-[600px] max-h-[800px]">
                    {sortedAlerts.length === 0 ? (
                      <div className="text-sm text-gray-500 flex items-center gap-2 h-full justify-center">
                        <ShieldCheck className="text-green-500 w-5 h-5 flex-shrink-0" />
                        No high or critical alerts currently.
                      </div>
                    ) : (
                      sortedAlerts.map(t => {
                        const isNew = newTxIds.has(t.transaction_id);
                        return (
                        <div 
                          key={t.transaction_id} 
                          onClick={() => setSelectedTx(t)}
                          className={`p-3 rounded-lg border text-sm cursor-pointer hover:shadow-md transition-all duration-500 ${getRiskColor(t.risk_tier)} ${isNew ? 'ring-2 ring-blue-500 scale-[1.02]' : ''}`}
                        >
                          <div className="flex justify-between font-bold mb-1 border-b border-black/10 pb-1">
                            <span>{t.risk_tier?.toUpperCase()}</span>
                            <span className="capitalize">{t.agent_action}</span>
                          </div>
                          <div className="font-mono text-xs mb-2 opacity-80 break-all">{t.transaction_id}</div>
                          <div className="bg-white/60 p-2 rounded text-xs font-medium text-gray-800">
                            {t.agent_reason || 'No reason provided'}
                          </div>
                        </div>
                      )})
                    )}
                  </div>
                </div>
              </div>
            </div>
          </>
        )}
      </div>

      {selectedTx && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50 backdrop-blur-sm">
          <div className="bg-white rounded-xl shadow-2xl w-full max-w-xl overflow-hidden border border-gray-200 animate-in fade-in zoom-in duration-200">
            <div className="p-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
              <h3 className="font-semibold text-lg flex items-center gap-2">
                <Search className="w-5 h-5 text-blue-600" />
                Transaction Analysis
              </h3>
              <button 
                onClick={() => setSelectedTx(null)}
                className="text-gray-400 hover:text-gray-900 bg-white hover:bg-gray-200 rounded-full p-1 transition-colors"
              >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
              </button>
            </div>
            <div className="p-6 space-y-6">
              <div className="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-lg border border-gray-100">
                <div>
                  <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Transaction ID</div>
                  <div className="font-mono text-sm break-all">{selectedTx.transaction_id}</div>
                </div>
                <div>
                  <div className="text-xs font-semibold text-gray-500 uppercase mb-1">Amount</div>
                  <div className="font-semibold text-xl text-gray-900">${selectedTx.amount?.toFixed(2)}</div>
                </div>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className={`p-4 rounded-lg border flex flex-col items-center justify-center ${getRiskColor(selectedTx.risk_tier)}`}>
                  <div className="text-xs uppercase font-bold opacity-70 mb-1">Assessed Risk Tier</div>
                  <div className="text-2xl font-black uppercase tracking-widest">{selectedTx.risk_tier}</div>
                </div>
                <div className="p-4 rounded-lg border border-gray-200 bg-white flex flex-col items-center justify-center shadow-sm">
                  <div className="text-xs uppercase font-bold text-gray-500 mb-1">Fraud Probability</div>
                  <div className="text-2xl font-black text-gray-900">{(selectedTx.fraud_probability * 100).toFixed(1)}%</div>
                </div>
              </div>

              <div className="bg-blue-50/50 rounded-lg border border-blue-100 p-5 mt-2">
                <div className="text-xs font-bold text-blue-800 mb-2 uppercase tracking-wide flex items-center gap-2">
                  <ShieldCheck className="w-4 h-4" /> Agent Decision Action
                </div>
                <div className="flex items-center gap-3 mb-3">
                  <span className="px-3 py-1 bg-white border border-gray-200 shadow-sm rounded-md font-bold capitalize text-gray-900 text-lg">
                    {selectedTx.agent_action}
                  </span>
                </div>
                <div className="text-sm text-gray-700 bg-white p-3 rounded border border-gray-200 italic shadow-inner">
                  "{selectedTx.agent_reason || 'No explanation generated.'}"
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
