import { useState, useEffect } from 'react';
import axios from 'axios';
import { Link } from 'react-router-dom';
import { AlertCircle, CheckCircle, Clock } from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

export default function Transactions() {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchTransactions = async () => {
      try {
        const response = await axios.get(`${API_URL}/transactions`);
        setTransactions(response.data);
      } catch (error) {
        console.error("Error fetching transactions:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchTransactions();
  }, []);

  const getRiskColor = (tier) => {
    switch(tier?.toUpperCase()) {
      case 'LOW': return 'bg-green-100 text-green-800 border-green-200';
      case 'MEDIUM': return 'bg-yellow-100 text-yellow-800 border-yellow-200';
      case 'HIGH': return 'bg-red-100 text-red-800 border-red-200';
      case 'CRITICAL': return 'bg-red-900 text-white border-red-800';
      default: return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  const getActionIcon = (action) => {
    switch(action?.toUpperCase()) {
      case 'APPROVE': return <CheckCircle className="h-4 w-4 text-green-500 mr-2" />;
      case 'BLOCK': return <AlertCircle className="h-4 w-4 text-red-500 mr-2" />;
      case 'MANUAL_REVIEW': return <Clock className="h-4 w-4 text-yellow-500 mr-2" />;
      default: return null;
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className="px-6 py-5 border-b border-gray-200 bg-gray-50 flex justify-between items-center">
        <h2 className="text-xl font-semibold text-gray-800">Recent Transactions</h2>
        <span className="text-sm text-gray-500">{transactions.length} total records</span>
      </div>
      
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 text-sm text-left">
          <thead className="bg-gray-50 text-gray-500 font-medium">
            <tr>
              <th scope="col" className="px-6 py-3 tracking-wider">Transaction ID</th>
              <th scope="col" className="px-6 py-3 tracking-wider">Amount</th>
              <th scope="col" className="px-6 py-3 tracking-wider">Risk Tier</th>
              <th scope="col" className="px-6 py-3 tracking-wider">Fraud Prob.</th>
              <th scope="col" className="px-6 py-3 tracking-wider">Agent Action</th>
              <th scope="col" className="px-6 py-3 tracking-wider text-right">Details</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {loading ? (
              <tr>
                <td colSpan="6" className="px-6 py-10 text-center text-gray-500">
                  <div className="flex justify-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                  </div>
                </td>
              </tr>
            ) : transactions.length === 0 ? (
              <tr>
                <td colSpan="6" className="px-6 py-10 text-center text-gray-500">
                  No transactions found. Make sure the backend is running and data exists.
                </td>
              </tr>
            ) : (
              transactions.map((tx, idx) => (
                <tr key={idx} className="hover:bg-gray-50 transition-colors">
                  <td className="px-6 py-4 whitespace-nowrap font-mono text-gray-700">
                    {tx.transaction_id ? tx.transaction_id.substring(0, 8) + '...' : 'N/A'}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap font-medium text-gray-900">
                    ${tx.amount?.toFixed(2)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2.5 py-1 inline-flex text-xs leading-5 font-semibold rounded-full border ${getRiskColor(tx.risk_tier)}`}>
                      {tx.risk_tier}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
                        <div 
                          className={`bg-${tx.fraud_probability > 0.5 ? 'red' : 'green'}-500 h-2 rounded-full`} 
                          style={{ width: `${Math.min(100, Math.max(0, tx.fraud_probability * 100))}%` }}
                        ></div>
                      </div>
                      <span className="text-gray-700">{(tx.fraud_probability * 100).toFixed(1)}%</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      {getActionIcon(tx.agent_action)}
                      <span className="text-gray-800">{tx.agent_action}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right">
                    <Link 
                      to={`/transactions/${tx.transaction_id}`}
                      className="text-blue-600 hover:text-blue-900 font-medium"
                    >
                      View
                    </Link>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
