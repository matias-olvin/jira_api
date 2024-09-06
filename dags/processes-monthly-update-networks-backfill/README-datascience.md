Networks Model v1

## NODES

We process all the nodes by doing - then we do the processing of the data. Then we filter the POIs we are interested
in (both location-wise and chain/sensitive-wise)

I took this approach so that the data I used in the testing(Milwaukee) is the same as in the US-expansion.

## EDGES

We process just the POIs we are interested in (chains and non-sensitive).

## TO DOs

- Code in a repository instead of only in the VM. VM to read from that repo.
- Add homes (for the edges observed, code is commented out)
- Add node features
- Ideally, we save the model and we retrain in the following month. (trying to get similar results instead of havin to
  smooth the data)