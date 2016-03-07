# telco-anomaly-detection-spark
Anomaly Detection in Telcos with Spark


# Internal Pieces

In general, the world is modeled as a flat two-dimensional space. For convenience, we may bound the coordinates and allow wrap-around to simplify the modeling of users, but we will assume that the circumference of the world is large enough that we only have to model RF propagation along a single shortest path between transmitter and receiver.

## User calling model

See docs/sip-refresh-state-machine.pdf for a picture of the user session state machine.

The idea behind the user calling model is that users start in a disconnected state. After a period time time, the user tries to initiate a call, transitioning from IDLE to CONNECTING (connect on the diagram). This transition sends a Hello message to the tower with the strongest signal. While in CONNECTING, the caller can receive FAIL (tower refused a connection) or CONNECT (tower accepted) messages or the caller can time out. For FAIL and timeout, Hello is sent to the next tower in the list of live towers.

In general, a new request for tower signal strengths is broadcast to all towers once every few seconds. This keeps signal strength reports coming in and what is used to construct the signal report table. If a tower doesn't report for a while, the last report from that tower will leak out of the table pretty quickly (in roughly 2+ heartbeats).

In the future, it would be better to have a limited list of towers to send requests for signal strength to. This can be done by having the towers learn which other towers are nearby. Then the caller can learn the neighborhood around all nearby towers pretty easily. Avoiding broadcast to all towers would help the scaling of this algorithm.

## Tower model

Towers are simple beasts who mostly just respond to the Hello and SignalReportRequest messages they get.

Before long, the Tower will need to be extended a bit to handle sending data to the MapR Streams that handle the simulated CDR messages from the network.
## Antenna model
Antennas are modeled using a product of an ellipse with one focus at the antenna and a cardiod with 2N lobes. This gives plausible looking antenna models.

For many purposes, setting the number of lobes to zero will be entirely good enough. 

An antenna can be modified if desired by moving the second focus of the antenna. This is a handy way to incrementally learn how to target particular locations using beaming.

An antenna can be asked how much power it will be able to direct to any point. This is expressed in dbm which is $20 \log_{10} P$ where P is the received power expressed in mW.
