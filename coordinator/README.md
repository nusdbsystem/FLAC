#### APIs

|       API        |                      Explanation                       | Thread-safe |
| :--------------: | :----------------------------------------------------: | :---------: |
|       Main       | Start the coordinator node with Args and config file. |      Y      |
| NewTX |     Create an object to handle a job fullNode clients.     |      T      |
|     PreRead      |     PreRead the data with a readCnt only transaction.     |      T      |
|    SubmitTxn     |   A common handler for four protocols. (write only)   |      T      |

- Y for thread-safe, T for thread-safe between transactions.


