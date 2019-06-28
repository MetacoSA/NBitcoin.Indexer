using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
    public class BalanceSheet
    {
        private readonly ChainBase _Chain;
        public ChainBase Chain
        {
            get
            {
                return _Chain;
            }
        }
        public BalanceSheet(IEnumerable<OrderedBalanceChange> changes, ChainBase chain)
        {
            if (chain == null)
                throw new ArgumentNullException("chain");
            _Chain = chain;

            var transactionsById = new Dictionary<uint256, OrderedBalanceChange>();
            List<OrderedBalanceChange> ignoredTransactions = new List<OrderedBalanceChange>();

            foreach (var trackedTx in changes)
            {
                int? txHeight = null;

                if (trackedTx.BlockId != null && chain.TryGetHeight(trackedTx.BlockId, out var height))
                {
                    txHeight = height;
                }
                if (trackedTx.BlockId != null && txHeight is null)
                {
                    _Prunable.Add(trackedTx);
                    continue;
                }

                if (transactionsById.TryGetValue(trackedTx.TransactionId, out var conflicted))
                {
                    if (ShouldReplace(trackedTx, conflicted))
                    {
                        ignoredTransactions.Add(conflicted);
                        transactionsById.Remove(trackedTx.TransactionId);
                        transactionsById.Add(trackedTx.TransactionId, trackedTx);
                    }
                    else
                    {
                        ignoredTransactions.Add(trackedTx);
                    }
                }
                else
                {
                    transactionsById.Add(trackedTx.TransactionId, trackedTx);
                }
            }

            // Let's resolve the double spents
            Dictionary<OutPoint, uint256> spentBy = new Dictionary<OutPoint, uint256>();
            foreach (var annotatedTransaction in transactionsById.Values.Where(r => r.BlockId != null))
            {
                foreach (var spent in annotatedTransaction.SpentOutpoints)
                {
                    // No way to have double spent in confirmed transactions
                    spentBy.Add(spent, annotatedTransaction.TransactionId);
                }
            }

            List<OrderedBalanceChange> replacedTransactions = new List<OrderedBalanceChange>();
        removeConflicts:
            HashSet<uint256> conflicts = new HashSet<uint256>();
            foreach (var annotatedTransaction in transactionsById.Values.Where(r => r.BlockId == null))
            {
                foreach (var spent in annotatedTransaction.SpentOutpoints)
                {
                    if (spentBy.TryGetValue(spent, out var conflictHash) &&
                        transactionsById.TryGetValue(conflictHash, out var conflicted))
                    {
                        if (conflicted == annotatedTransaction)
                            goto nextTransaction;
                        if (conflicts.Contains(conflictHash))
                        {
                            spentBy.Remove(spent);
                            spentBy.Add(spent, annotatedTransaction.TransactionId);
                        }
                        else if (ShouldReplace(annotatedTransaction, conflicted))
                        {
                            conflicts.Add(conflictHash);
                            spentBy.Remove(spent);
                            spentBy.Add(spent, annotatedTransaction.TransactionId);

                            if (conflicted.BlockId == null && annotatedTransaction.BlockId == null)
                            {
                                replacedTransactions.Add(conflicted);
                            }
                            else
                            {
                                ignoredTransactions.Add(conflicted);
                            }
                        }
                        else
                        {
                            conflicts.Add(annotatedTransaction.TransactionId);
                            if (conflicted.BlockId == null && annotatedTransaction.BlockId == null)
                            {
                                replacedTransactions.Add(annotatedTransaction);
                            }
                            else
                            {
                                ignoredTransactions.Add(annotatedTransaction);
                            }
                        }
                    }
                    else
                    {
                        spentBy.Add(spent, annotatedTransaction.TransactionId);
                    }
                }
            nextTransaction:;
            }

            foreach (var e in conflicts)
            {
                _Prunable.Add(transactionsById[e]);
                transactionsById.Remove(e);
            }
            if (conflicts.Count != 0)
                goto removeConflicts;

            // Topological sort
            var sortedTrackedTransactions = transactionsById.Values.TopologicalSort();
            // Remove all ignored transaction from the database
            foreach (var ignored in ignoredTransactions)
            {
                _Prunable.Add(ignored);
            }

            _All = sortedTrackedTransactions;
            _All.Reverse();
            _Confirmed = sortedTrackedTransactions.Where(s => s.BlockId != null).ToList();
            _Unconfirmed = sortedTrackedTransactions.Where(s => s.BlockId == null).ToList();
        }

        private bool ShouldReplace(OrderedBalanceChange annotatedTransaction, OrderedBalanceChange conflicted)
        {
            return OrderBalanceChangeComparer.Instance.Compare(annotatedTransaction, conflicted) == -1;
        }


        private readonly List<OrderedBalanceChange> _Unconfirmed;
        public List<OrderedBalanceChange> Unconfirmed
        {
            get
            {
                return _Unconfirmed;
            }
        }
        private readonly List<OrderedBalanceChange> _Confirmed;
        public List<OrderedBalanceChange> Confirmed
        {
            get
            {
                return _Confirmed;
            }
        }

        private readonly List<OrderedBalanceChange> _All;
        public List<OrderedBalanceChange> All
        {
            get
            {
                return _All;
            }
        }
        private readonly List<OrderedBalanceChange> _Prunable = new List<OrderedBalanceChange>();
        public List<OrderedBalanceChange> Prunable
        {
            get
            {
                return _Prunable;
            }
        }

    }
}
