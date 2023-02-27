## Manager

The participant component for FLAC protocol.
- It serves as a bridge between request fullNode coordinator and local store.
- One `ParticipantStmt` with one `Shard` is needed for each node.

The unit tests for this component is used for development. If you want to run them, please change to local test setup

#### APIs

|          API          |                    Explanation                     | Thread-safe |
| :-------------------: | :------------------------------------------------: | :---------: |
|         Main          |  Start the participant node with Args and config file.  |      Y      |
|     Break/Recover     | Used for local test to simulate the crash failure. |      Y      |
|        PreRead        | Handles a readCnt only transaction and return values. |      T      |
| PreWrite/Commit/Abort |                  Common handlers                   |      T      |
|         Agree         |                    3PC handler                     |      T      |
|        ST1        |     For FLAC propose phase, return the results      |      T      |


- Y for thread-safe, T for thread-safe between transactions.
