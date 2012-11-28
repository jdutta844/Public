The zip containing three map reduce programs
The java source files are in src/jay/mr/collective
Package name used jay.mr.collective in all java program files
Environment - JDK  1.6, Hadoop 0.20.2

1.  Program - QuoteCountByName.java
              This program counts the no of quotes for each person
    Input -   folder name input
    Output -  folder name outputQCBN, file name part-r-00000
    
    
2.  Program - CountOfWords.java
              This program counts the no of words in the quotes irrespective of persons
    Input -   folder name input
    Output -  folder name outputCOW, file name part-r-00000
    

3.  Program - ChainedWordCount.java
              This program counts the no of words in the quotes for each persons, and shows in one line per person
    Input -   folder name input
    Output -  folder name outputCWC, file name part-r-00000
    
    Phases : 3 map reduce phases, intermediate output folders are outputPhase1 and outputPhase2