"""Recursive extrema confirmation using only the supplied historical prefix."""
import numpy as np

def confirmed_levels(values,kind,max_level=6):
    """Confirm first/interior points only; rightmost points await a neighbor.

    Cutoff is the last lower-order point whose level membership is decided.
    At each depth the low/high sequences can have different cutoffs.
    """
    active=np.arange(len(values),dtype=int)
    levels={}
    for level in range(1,max_level+1):
        if len(active)<2:
            levels[level]={'points':np.array([],dtype=int),'cutoff':-1};active=np.array([],dtype=int);continue
        prices=values[active]
        cmp=np.less if kind=='L' else np.greater
        mask=np.zeros(len(active),dtype=bool)
        mask[0]=cmp(prices[0],prices[1])
        mask[1:-1]=cmp(prices[1:-1],prices[:-2]) & cmp(prices[1:-1],prices[2:])
        cutoff=int(active[-2]);active=active[mask]
        levels[level]={'points':active.copy(),'cutoff':cutoff}
    return levels
