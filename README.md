# utl-very-fast-processing-using-sql-arrays-on-large-core-systems
Very fast processing using sql arrays on large core systems
    Very fast processing using sql arrays on large core systems?

    github
    https://tinyurl.com/y8x565ne
    https://github.com/rogerjdeangelis/utl-very-fast-processing-using-sql-arrays-on-large-core-systems


    StackOverflow
    https://stackoverflow.com/questions/61459939/looping-with-proc-sql

    Some large Teradata Clusters have thousands of cores.

    Potentially a big data problem?

    macros
    https://tinyurl.com/y9nfugth
    https://github.com/rogerjdeangelis/utl-macros-used-in-many-of-rogerjdeangelis-repositories
    *_                   _
    (_)_ __  _ __  _   _| |_
    | | '_ \| '_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    ;

    data have ;
      input date$ acct$ (seg_1-seg_5) ($)  abc_1-abc_5;
    cards4;
    1/20 A X X X X Y 0 0 1 1 1
    1/20 B X Y X X Y 1 1 0 1 1
    1/20 C Y Y X X X 1 0 1 0 1
    ;;;;
    run;quit;

    Up to 40 obs WORK.HAVE total obs=3

     DATE    ACCT    SEG_1    SEG_2    SEG_3    SEG_4    SEG_5    ABC_1    ABC_2    ABC_3    ABC_4    ABC_5

     1/20     A        X        X        X        X        Y        0        0        1        1        1
     1/20     B        X        Y        X        X        Y        1        1        0        1        1
     1/20     C        Y        Y        X        X        X        1        0        1        0        1

    *           _
     _ __ _   _| | ___  ___
    | '__| | | | |/ _ \/ __|
    | |  | |_| | |  __/\__ \
    |_|   \__,_|_|\___||___/

    ;

    Lets do the first create table in the loop

    proc sql;
          create
             table want_1 as
          select
              distinct seg_1 as segment
             ,(count(acct)) as count_1
          from
              have
          where
              abc_2 = 0 and    /*
              abc_3 = 1        /* ob 1  True     abc_2 =  0 and abc_3 =  1
          group                /* ob 2  False    abc_2 ne 0 and abc_3 ne 1
              by seg_1         /* ob 3  True     abc_2 =  0 and abc_3 =  1
    ;quit;

    *            _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| '_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    ;

    * Agrees witht the rule

    WORK.WANT_1 total obs=2

     SEGMENT    COUNT_1

        X          1
        Y          1

    *
     _ __  _ __ ___   ___ ___  ___ ___
    | '_ \| '__/ _ \ / __/ _ \/ __/ __|
    | |_) | | | (_) | (_|  __/\__ \__ \
    | .__/|_|  \___/ \___\___||___/___/
    |_|
    ;

    %array(i,values=1-3);
    %array(j,values=2-4);
    %array(k,values=3-5);

    proc sql;

      proc sql;

      %do_over(i j k,phrase=%str(
        create table want_?i as
        select distinct seg_?i as segment, (count(acct)) as count_?i
        from have
        where abc_?j = 0 and abc_?k = 1
        group by seg_?i;
        )
      );

    quit;

    *_
    | | ___   __ _
    | |/ _ \ / _` |
    | | (_) | (_| |
    |_|\___/ \__, |
             |___/
    ;

    52      proc sql;
    53      %do_over(i j k,phrase=%str(
    54        create table want_?i as
    55        select distinct seg_?i as segment, (count(acct)) as count_?i
    56        from have
    57        where abc_?j = 0 and abc_?k = 1
    58        group by seg_?i;
    59        )
    60      );
    NOTE: Table WORK.WANT_1 created, with 2 rows and 2 columns.

    NOTE: Table WORK.WANT_2 created, with 1 rows and 2 columns.

    NOTE: Table WORK.WANT_3 created, with 1 rows and 2 columns.


    *              _
      ___ ___   __| | ___    __ _  ___ _ __
     / __/ _ \ / _` |/ _ \  / _` |/ _ \ '_ \
    | (_| (_) | (_| |  __/ | (_| |  __/ | | |
     \___\___/ \__,_|\___|  \__, |\___|_| |_|
                            |___/
    ;

    %utlnopts;

    data _null_;
      put
      %do_over(i j k ,phrase=%str("
        proc sql;
        create table want_?i as
        select distinct seg_?i as segment, (count(acct)) as count_?i
        from have
        where abc_?j = 0 and abc_?k = 1
        group by seg_?i;
        " / )
      );
    run;quit;

    proc sql;    create table want_1 as    select distinct seg_1 as segment, (count(acct)) as count_1    from have    where abc_2 = 0 and abc_3 = 1    group by se
    proc sql;    create table want_2 as    select distinct seg_2 as segment, (count(acct)) as count_2    from have    where abc_3 = 0 and abc_4 = 1    group by se
    proc sql;    create table want_3 as    select distinct seg_3 as segment, (count(acct)) as count_3    from have    where abc_4 = 0 and abc_5 = 1    group by se

    %utlnopts;

    %array(i,values=1-81);
    %array(j,values=2-82);
    %array(k,values=3-83);

    data _null_;
      put
      %do_over(i j k ,phrase=%str("
        create table want_?i as
        select distinct seg_?i as segment, (count(acct)) as count_?i
        from have
        where abc_?j = 0 and abc_?k = 1
        group by seg_?i;
        " / )
      );
    run;quit;

    *              _                            ___ _____
      ___ ___   __| | ___    __ _  ___ _ __    ( _ )___ /
     / __/ _ \ / _` |/ _ \  / _` |/ _ \ '_ \   / _ \ |_ \
    | (_| (_) | (_| |  __/ | (_| |  __/ | | | | (_) |__) |
     \___\___/ \__,_|\___|  \__, |\___|_| |_|  \___/____/
                            |___/
    ;

    create table want_1 as select distinct seg_1 as segment, (count(acct)) as count_1 from have where abc_2 = 0 and abc_3 = 1 group by seg_1;
    create table want_2 as select distinct seg_2 as segment, (count(acct)) as count_2 from have where abc_3 = 0 and abc_4 = 1 group by seg_2;
    create table want_3 as select distinct seg_3 as segment, (count(acct)) as count_3 from have where abc_4 = 0 and abc_5 = 1 group by seg_3;
    create table want_4 as select distinct seg_4 as segment, (count(acct)) as count_4 from have where abc_5 = 0 and abc_6 = 1 group by seg_4;
    create table want_5 as select distinct seg_5 as segment, (count(acct)) as count_5 from have where abc_6 = 0 and abc_7 = 1 group by seg_5;
    create table want_6 as select distinct seg_6 as segment, (count(acct)) as count_6 from have where abc_7 = 0 and abc_8 = 1 group by seg_6;
    create table want_7 as select distinct seg_7 as segment, (count(acct)) as count_7 from have where abc_8 = 0 and abc_9 = 1 group by seg_7;
    create table want_8 as select distinct seg_8 as segment, (count(acct)) as count_8 from have where abc_9 = 0 and abc_10 = 1 group by seg_8;
    create table want_9 as select distinct seg_9 as segment, (count(acct)) as count_9 from have where abc_10 = 0 and abc_11 = 1 group by seg_9;
    create table want_10 as select distinct seg_10 as segment, (count(acct)) as count_10 from have where abc_11 = 0 and abc_12 = 1 group by seg_10;
    create table want_11 as select distinct seg_11 as segment, (count(acct)) as count_11 from have where abc_12 = 0 and abc_13 = 1 group by seg_11;
    create table want_12 as select distinct seg_12 as segment, (count(acct)) as count_12 from have where abc_13 = 0 and abc_14 = 1 group by seg_12;
    create table want_13 as select distinct seg_13 as segment, (count(acct)) as count_13 from have where abc_14 = 0 and abc_15 = 1 group by seg_13;
    create table want_14 as select distinct seg_14 as segment, (count(acct)) as count_14 from have where abc_15 = 0 and abc_16 = 1 group by seg_14;
    create table want_15 as select distinct seg_15 as segment, (count(acct)) as count_15 from have where abc_16 = 0 and abc_17 = 1 group by seg_15;
    create table want_16 as select distinct seg_16 as segment, (count(acct)) as count_16 from have where abc_17 = 0 and abc_18 = 1 group by seg_16;
    create table want_17 as select distinct seg_17 as segment, (count(acct)) as count_17 from have where abc_18 = 0 and abc_19 = 1 group by seg_17;
    create table want_18 as select distinct seg_18 as segment, (count(acct)) as count_18 from have where abc_19 = 0 and abc_20 = 1 group by seg_18;
    create table want_19 as select distinct seg_19 as segment, (count(acct)) as count_19 from have where abc_20 = 0 and abc_21 = 1 group by seg_19;
    create table want_20 as select distinct seg_20 as segment, (count(acct)) as count_20 from have where abc_21 = 0 and abc_22 = 1 group by seg_20;
    create table want_21 as select distinct seg_21 as segment, (count(acct)) as count_21 from have where abc_22 = 0 and abc_23 = 1 group by seg_21;
    create table want_22 as select distinct seg_22 as segment, (count(acct)) as count_22 from have where abc_23 = 0 and abc_24 = 1 group by seg_22;
    create table want_23 as select distinct seg_23 as segment, (count(acct)) as count_23 from have where abc_24 = 0 and abc_25 = 1 group by seg_23;
    create table want_24 as select distinct seg_24 as segment, (count(acct)) as count_24 from have where abc_25 = 0 and abc_26 = 1 group by seg_24;
    create table want_25 as select distinct seg_25 as segment, (count(acct)) as count_25 from have where abc_26 = 0 and abc_27 = 1 group by seg_25;
    create table want_26 as select distinct seg_26 as segment, (count(acct)) as count_26 from have where abc_27 = 0 and abc_28 = 1 group by seg_26;
    create table want_27 as select distinct seg_27 as segment, (count(acct)) as count_27 from have where abc_28 = 0 and abc_29 = 1 group by seg_27;
    create table want_28 as select distinct seg_28 as segment, (count(acct)) as count_28 from have where abc_29 = 0 and abc_30 = 1 group by seg_28;
    create table want_29 as select distinct seg_29 as segment, (count(acct)) as count_29 from have where abc_30 = 0 and abc_31 = 1 group by seg_29;
    create table want_30 as select distinct seg_30 as segment, (count(acct)) as count_30 from have where abc_31 = 0 and abc_32 = 1 group by seg_30;
    create table want_31 as select distinct seg_31 as segment, (count(acct)) as count_31 from have where abc_32 = 0 and abc_33 = 1 group by seg_31;
    create table want_32 as select distinct seg_32 as segment, (count(acct)) as count_32 from have where abc_33 = 0 and abc_34 = 1 group by seg_32;
    create table want_33 as select distinct seg_33 as segment, (count(acct)) as count_33 from have where abc_34 = 0 and abc_35 = 1 group by seg_33;
    create table want_34 as select distinct seg_34 as segment, (count(acct)) as count_34 from have where abc_35 = 0 and abc_36 = 1 group by seg_34;
    create table want_35 as select distinct seg_35 as segment, (count(acct)) as count_35 from have where abc_36 = 0 and abc_37 = 1 group by seg_35;
    create table want_36 as select distinct seg_36 as segment, (count(acct)) as count_36 from have where abc_37 = 0 and abc_38 = 1 group by seg_36;
    create table want_37 as select distinct seg_37 as segment, (count(acct)) as count_37 from have where abc_38 = 0 and abc_39 = 1 group by seg_37;
    create table want_38 as select distinct seg_38 as segment, (count(acct)) as count_38 from have where abc_39 = 0 and abc_40 = 1 group by seg_38;
    create table want_39 as select distinct seg_39 as segment, (count(acct)) as count_39 from have where abc_40 = 0 and abc_41 = 1 group by seg_39;
    create table want_40 as select distinct seg_40 as segment, (count(acct)) as count_40 from have where abc_41 = 0 and abc_42 = 1 group by seg_40;
    create table want_41 as select distinct seg_41 as segment, (count(acct)) as count_41 from have where abc_42 = 0 and abc_43 = 1 group by seg_41;
    create table want_42 as select distinct seg_42 as segment, (count(acct)) as count_42 from have where abc_43 = 0 and abc_44 = 1 group by seg_42;
    create table want_43 as select distinct seg_43 as segment, (count(acct)) as count_43 from have where abc_44 = 0 and abc_45 = 1 group by seg_43;
    create table want_44 as select distinct seg_44 as segment, (count(acct)) as count_44 from have where abc_45 = 0 and abc_46 = 1 group by seg_44;
    create table want_45 as select distinct seg_45 as segment, (count(acct)) as count_45 from have where abc_46 = 0 and abc_47 = 1 group by seg_45;
    create table want_46 as select distinct seg_46 as segment, (count(acct)) as count_46 from have where abc_47 = 0 and abc_48 = 1 group by seg_46;
    create table want_47 as select distinct seg_47 as segment, (count(acct)) as count_47 from have where abc_48 = 0 and abc_49 = 1 group by seg_47;
    create table want_48 as select distinct seg_48 as segment, (count(acct)) as count_48 from have where abc_49 = 0 and abc_50 = 1 group by seg_48;
    create table want_49 as select distinct seg_49 as segment, (count(acct)) as count_49 from have where abc_50 = 0 and abc_51 = 1 group by seg_49;
    create table want_50 as select distinct seg_50 as segment, (count(acct)) as count_50 from have where abc_51 = 0 and abc_52 = 1 group by seg_50;
    create table want_51 as select distinct seg_51 as segment, (count(acct)) as count_51 from have where abc_52 = 0 and abc_53 = 1 group by seg_51;
    create table want_52 as select distinct seg_52 as segment, (count(acct)) as count_52 from have where abc_53 = 0 and abc_54 = 1 group by seg_52;
    create table want_53 as select distinct seg_53 as segment, (count(acct)) as count_53 from have where abc_54 = 0 and abc_55 = 1 group by seg_53;
    create table want_54 as select distinct seg_54 as segment, (count(acct)) as count_54 from have where abc_55 = 0 and abc_56 = 1 group by seg_54;
    create table want_55 as select distinct seg_55 as segment, (count(acct)) as count_55 from have where abc_56 = 0 and abc_57 = 1 group by seg_55;
    create table want_56 as select distinct seg_56 as segment, (count(acct)) as count_56 from have where abc_57 = 0 and abc_58 = 1 group by seg_56;
    create table want_57 as select distinct seg_57 as segment, (count(acct)) as count_57 from have where abc_58 = 0 and abc_59 = 1 group by seg_57;
    create table want_58 as select distinct seg_58 as segment, (count(acct)) as count_58 from have where abc_59 = 0 and abc_60 = 1 group by seg_58;
    create table want_59 as select distinct seg_59 as segment, (count(acct)) as count_59 from have where abc_60 = 0 and abc_61 = 1 group by seg_59;
    create table want_60 as select distinct seg_60 as segment, (count(acct)) as count_60 from have where abc_61 = 0 and abc_62 = 1 group by seg_60;
    create table want_61 as select distinct seg_61 as segment, (count(acct)) as count_61 from have where abc_62 = 0 and abc_63 = 1 group by seg_61;
    create table want_62 as select distinct seg_62 as segment, (count(acct)) as count_62 from have where abc_63 = 0 and abc_64 = 1 group by seg_62;
    create table want_63 as select distinct seg_63 as segment, (count(acct)) as count_63 from have where abc_64 = 0 and abc_65 = 1 group by seg_63;
    create table want_64 as select distinct seg_64 as segment, (count(acct)) as count_64 from have where abc_65 = 0 and abc_66 = 1 group by seg_64;
    create table want_65 as select distinct seg_65 as segment, (count(acct)) as count_65 from have where abc_66 = 0 and abc_67 = 1 group by seg_65;
    create table want_66 as select distinct seg_66 as segment, (count(acct)) as count_66 from have where abc_67 = 0 and abc_68 = 1 group by seg_66;
    create table want_67 as select distinct seg_67 as segment, (count(acct)) as count_67 from have where abc_68 = 0 and abc_69 = 1 group by seg_67;
    create table want_68 as select distinct seg_68 as segment, (count(acct)) as count_68 from have where abc_69 = 0 and abc_70 = 1 group by seg_68;
    create table want_69 as select distinct seg_69 as segment, (count(acct)) as count_69 from have where abc_70 = 0 and abc_71 = 1 group by seg_69;
    create table want_70 as select distinct seg_70 as segment, (count(acct)) as count_70 from have where abc_71 = 0 and abc_72 = 1 group by seg_70;
    create table want_71 as select distinct seg_71 as segment, (count(acct)) as count_71 from have where abc_72 = 0 and abc_73 = 1 group by seg_71;
    create table want_72 as select distinct seg_72 as segment, (count(acct)) as count_72 from have where abc_73 = 0 and abc_74 = 1 group by seg_72;
    create table want_73 as select distinct seg_73 as segment, (count(acct)) as count_73 from have where abc_74 = 0 and abc_75 = 1 group by seg_73;
    create table want_74 as select distinct seg_74 as segment, (count(acct)) as count_74 from have where abc_75 = 0 and abc_76 = 1 group by seg_74;
    create table want_75 as select distinct seg_75 as segment, (count(acct)) as count_75 from have where abc_76 = 0 and abc_77 = 1 group by seg_75;
    create table want_76 as select distinct seg_76 as segment, (count(acct)) as count_76 from have where abc_77 = 0 and abc_78 = 1 group by seg_76;
    create table want_77 as select distinct seg_77 as segment, (count(acct)) as count_77 from have where abc_78 = 0 and abc_79 = 1 group by seg_77;
    create table want_78 as select distinct seg_78 as segment, (count(acct)) as count_78 from have where abc_79 = 0 and abc_80 = 1 group by seg_78;
    create table want_79 as select distinct seg_79 as segment, (count(acct)) as count_79 from have where abc_80 = 0 and abc_81 = 1 group by seg_79;
    create table want_80 as select distinct seg_80 as segment, (count(acct)) as count_80 from have where abc_81 = 0 and abc_82 = 1 group by seg_80;
    create table want_81 as select distinct seg_81 as segment, (count(acct)) as count_81 from have where abc_82 = 0 and abc_83 = 1 group by seg_81;
