<!-- pandoc rack-rolling.md -f markdown+tex_math_dollars -o rack-rolling.pdf -->

Consider a cluster of brokers $B=\{1...N\}$ hosting replicas of a set partitions $P=\{p_1, p_2, p_3, ...\}$.

We wish to restart the set of brokers $R \subseteq B$ without impacting the availability of any partitions in $P$ to `acks=all` producers.

The simplest way to do that is one broker at a time, checking for whether that individual broker, $b$, impacts availability. 
Whether restarting a broker will impact availability it determined by

$$
rollable(b) = TODO
$$

While this is safe and simple it will be slow when the $|B|$ is large. 
We can restart brokers in parallel, but we need a safety condition which guarantees producer availability for all the partitions with replicas on the brokers which get restarted at the same time.

# A sufficient condition

It's definitely safe to parallel roll a set of brokers if they have no partitions in common 
(if they have no partitions in common then they can't have any replicas of those partitions in common either).
We can restart brokers in parallel by partitioning the _rollable_ brokers in $R$, that is ${b \in R : rollable(b)}$, by the equivalence relation `share_any_partition`. 
When all replicas of all partitions have be assigned in a rack-aware way then brokers in the same rack trivially share no partitions, and so racks provide a safe partitioning.
However, nothing in a broker, controller or cruise control is able to enforce the rack-aware property.
So either the property needs to be assumed to be true, or we need to prove it is true each time we're about to roll a rack.
In practice assuming this property is not safe.
Even if CC is being used and rack aware replicas is a hard goal we can't be certain that other tooling hasn't reassigned some replicas since the last rebalance, or that no topics have been created in a rack-unaware way.

# A weaker condition

The `share_any_partition` relation is actually stronger that is necessary.
For example we could safely simultaneously roll brokers $b_1$ and $b_2$ which each have a replica of $p_i$ when:

1. $\{b_1, b_2\} \cap isr(p_i) = \{\}$ (the brokers are not in the ISR for $p$, so restarting them cannot impact the ISR)
2. $|isr(p_i)| - min\_isr(p_i) \ge 2$ (the ISR is big enough to tolerate removing both $b_1$ and $b_2$)

More generally, we can safely restart any set $C \subseteq replicas(p_i)$ without impacting `acks=all` producers of $p_i$ when:

$$
0 \lt |C \cap isr(p_i)| \le |isr(p_i)| - min\_isr(p_i)
$$

<!--And applying this to all partitions:

$$
\bigcap_{p_i \in P} \mathcal{P}(replicas(pi) : \{0 \lt |replicas(pi) \cap isr(p_i)| \le |isr(p_i)| - min\_isr(p_i)\})
$$-->

We can then ask: "Is it practical to compute the best $c$ under some cost model, or is rack-aware rolling usually good enough?"

Considations include:

1. How long will it take to roll $R$? This is what we're trying to minimise by doing parallel rolling.
2. How many brokers should we roll at a time? Rolling many brokers places more strain on the unrolled brokers, since there's more catch up needed. It also increases the impact of a broker crash among the unrolled brokers.
3. How often might using the weaker condition allow parallel rolling where the stronger condition prevented it?



numRacks = 3
numBrokers = 7
numTopics = 1
numPartitionsPerTopic = 10
rf = 3
Broker[id=0, rack=0, replicas=[t1-0, t1-5, t1-6, t1-7]]
Broker[id=1, rack=1, replicas=[t1-0, t1-1, t1-6, t1-7, t1-8]]
Broker[id=2, rack=2, replicas=[t1-0, t1-1, t1-2, t1-7, t1-8, t1-9]]
Broker[id=3, rack=0, replicas=[t1-1, t1-2, t1-3, t1-8, t1-9]]
Broker[id=4, rack=1, replicas=[t1-2, t1-3, t1-4, t1-9]]
Broker[id=5, rack=2, replicas=[t1-3, t1-4, t1-5]]
Broker[id=6, rack=0, replicas=[t1-4, t1-5, t1-6]]
## [0, 3, 4]
## [2, 6]
## [1, 5]
[0, 3, 4]

Process finished with exit code 0

