import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import axios from 'axios';
import { ArrowLeft, FileJson, AlertTriangle, ShieldAlert, Activity } from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export default function TransactionDetail() {
  const { id } = useParams();
  const [transaction, setTransaction] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchTransaction = async () => {
      try {
        const response = await axios.get(`${API_URL}/transactions/${id}`);
        setTransaction(response.data);
      } catch (err) {
        setError(err.response?.status === 404 ? 'Transaction not found' : 'Failed to fetch transaction details');
      } finally {
        setLoading(false);
      }
    };

    fetchTransaction();
  }, [id]);

  if (loading) {
    return (
      <div className="flex justify-center items-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error || !transaction) {
    return (
      <div className="bg-red-50 border-l-4 border-red-400 p-4 rounded-md">
        <div className="flex">
          <AlertTriangle className="h-5 w-5 text-red-400" />
          <div className="ml-3">
            <h3 className="text-sm font-medium text-red-800">Error</h3>
            <p className="text-sm text-red-700 mt-1">{error}</p>
            <div className="mt-4">
              <Link to="/transactions" className="text-sm font-medium text-red-800 hover:text-red-900 border border-red-200 px-3 py-1 rounded">
                &larr; Back to Transactions
              </Link>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center space-x-4">
        <Link to="/transactions" className="p-2 hover:bg-gray-100 rounded-full transition-colors">
          <ArrowLeft className="h-5 w-5 text-gray-600" />
        </Link>
        <h1 className="text-2xl font-bold text-gray-900">Transaction Details</h1>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* Core Info */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 lg:col-span-1">
          <h2 className="text-lg font-semibold text-gray-800 mb-4 flex items-center">
            <Activity className="h-5 w-5 mr-2 text-blue-500" />
            Analysis Summary
          </h2>
          
          <div className="space-y-4">
            <div>
              <p className="text-sm text-gray-500">Transaction ID</p>
              <p className="font-mono text-gray-900 font-medium break-all">{transaction.transaction_id || 'N/A'}</p>
            </div>
            
            <div className="pt-2 border-t border-gray-100">
              <p className="text-sm text-gray-500 mb-1">Agent Reason</p>
              <div className="bg-blue-50 text-blue-800 p-3 rounded-lg text-sm italic">
                "{transaction.agent_reason || 'No specific reason provided'}"
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 pt-2 border-t border-gray-100">
              <div>
                <p className="text-sm text-gray-500">Action Taken</p>
                <div className="mt-1">
                  <span className="px-3 py-1 text-sm font-semibold rounded-full bg-slate-100 text-slate-800 border border-slate-200">
                    {transaction.agent_action || 'UNKNOWN'}
                  </span>
                </div>
              </div>
              <div>
                <p className="text-sm text-gray-500">Risk Tier</p>
                <div className="mt-1">
                  <span className={`px-3 py-1 text-sm font-semibold rounded-full border ${
                    transaction.risk_tier === 'CRITICAL' ? 'bg-red-900 text-white border-red-800' :
                    transaction.risk_tier === 'HIGH' ? 'bg-red-100 text-red-800 border-red-200' :
                    transaction.risk_tier === 'MEDIUM' ? 'bg-yellow-100 text-yellow-800 border-yellow-200' :
                    'bg-green-100 text-green-800 border-green-200'
                  }`}>
                    {transaction.risk_tier || 'LOW'}
                  </span>
                </div>
              </div>
            </div>

            <div className="pt-2 border-t border-gray-100">
              <p className="text-sm text-gray-500 mb-1">Fraud Probability</p>
              <div className="flex items-center">
                <span className="text-2xl font-bold text-gray-900 mr-3">
                  {transaction.fraud_probability ? (transaction.fraud_probability * 100).toFixed(2) : 0}%
                </span>
                <div className="flex-1 bg-gray-200 rounded-full h-2.5">
                  <div 
                    className={`h-2.5 rounded-full ${transaction.fraud_probability > 0.5 ? 'bg-red-500' : 'bg-green-500'}`} 
                    style={{ width: `${Math.min(100, Math.max(0, (transaction.fraud_probability || 0) * 100))}%` }}
                  ></div>
                </div>
              </div>
            </div>
            
          </div>
        </div>

        {/* JSON Payload */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 lg:col-span-2 flex flex-col">
          <h2 className="text-lg font-semibold text-gray-800 mb-4 flex items-center">
            <FileJson className="h-5 w-5 mr-2 text-purple-500" />
            Raw Payload Data
          </h2>
          <div className="flex-grow bg-slate-900 rounded-lg p-4 overflow-auto">
            <pre className="text-green-400 font-mono text-sm">
              {JSON.stringify(transaction, null, 2)}
            </pre>
          </div>
        </div>
        
      </div>
    </div>
  );
}
