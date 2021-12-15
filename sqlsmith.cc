#include "config.h"

#include <iostream>
#include <chrono>

#ifndef HAVE_BOOST_REGEX
#include <regex>
#else
#include <boost/regex.hpp>
using boost::regex;
using boost::smatch;
using boost::regex_match;
#endif

#include <thread>
#include <typeinfo>

#include "random.hh"
#include "grammar.hh"
#include "relmodel.hh"
#include "schema.hh"
#include "gitrev.h"

#include "log.hh"
#include "dump.hh"
#include "impedance.hh"
#include "dut.hh"

#ifdef HAVE_LIBSQLITE3
#include "sqlite.hh"
#endif

#ifdef HAVE_MONETDB
#include "monetdb.hh"
#endif

#include "postgres.hh"

using namespace std;

using namespace std::chrono;

extern "C" {
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
}

struct NullBuffer : public std::streambuf
{
public:
    int overflow(int c) { return c; }
};

struct NullStream : public std::ostream {
public:
    NullStream() : std::ostream(&m_sb){}
private:
    NullBuffer m_sb;
};

/* stream to output Metadata discovery details for Postgres */
std::ostream *pg_mtd_info_stream;

/* make the cerr logger globally accessible so we can emit one last
   report on SIGINT */
cerr_logger *global_cerr_logger;

extern "C" void cerr_log_handler(int)
{
  if (global_cerr_logger)
    global_cerr_logger->report();
  exit(1);
}

int main(int argc, char *argv[])
{
  cerr << PACKAGE_NAME " " GITREV << endl;

  pg_mtd_info_stream = new NullStream();
  map<string,string> options;
  regex optregex("--(help|log-to|verbose|target|sqlite|monetdb|version|dump-all-graphs|dump-all-queries|seed|dry-run|max-queries|rng-state|exclude-catalog|pg-mtd-info|types-to-include|tables-to-include|routines-to-include|aggregates-to-include)(?:=((?:.|\n)*))?");
  
  for(char **opt = argv+1 ;opt < argv+argc; opt++) {
    smatch match;
    string s(*opt);
    if (regex_match(s, match, optregex)) {
      options[string(match[1])] = match[2];
    } else {
      cerr << "Cannot parse option: " << *opt << endl;
      options["help"] = "";
    }
  }

  if (options.count("help")) {
    cerr <<
      "    --target=connstr     postgres database to send queries to" << endl <<
#ifdef HAVE_LIBSQLITE3
      "    --sqlite=URI         SQLite database to send queries to" << endl <<
#endif
#ifdef HAVE_MONETDB
      "    --monetdb=connstr    MonetDB database to send queries to" <<endl <<
#endif
      "    --log-to=connstr        log errors to postgres database" << endl <<
      "    --seed=int              seed RNG with specified int instead of PID" << endl <<
      "    --dump-all-queries      print queries as they are generated" << endl <<
      "    --dump-all-graphs       dump generated ASTs" << endl <<
      "    --dry-run               print queries instead of executing them" << endl <<
      "    --exclude-catalog       don't generate queries using catalog relations" << endl <<
      "    --pg-mtd-info           print Postgres Metadata Info to cerr" << endl <<
      "    --types-to-include      specify configuration file that white-lists types" << endl <<
      "    --tables-to-include     specify names of tables that should be in the generated SQL" << endl <<
      "    --routines-to-include   specify names of routines that should be in the generated SQL" << endl <<
      "    -–aggregates-to_include specify the aggregates that should be in the generated SQL" << endl <<
      "    --max-queries=long      terminate after generating this many queries" << endl <<
      "    --rng-state=string      deserialize dumped rng state" << endl <<
      "    --verbose               emit progress output" << endl <<
      "    --version               print version information and exit" << endl <<
      "    --help                  print available command line options and exit" << endl;
    return 0;
  } else if (options.count("version")) {
    return 0;
  }

  try
    {
      shared_ptr<schema> schema;
      if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
	schema = make_shared<schema_sqlite>(options["sqlite"], options.count("exclude-catalog"));
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
	return 1;
#endif
      }
      else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB
	schema = make_shared<schema_monetdb>(options["monetdb"]);
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
	return 1;
#endif
      }
      else {

          if(options.count("pg-mtd-info")){
              pg_mtd_info_stream = &cerr;
          }

          std::set<std::string> typesToInclude;
          if (options.count("types-to-include")) {
              cerr << "File containing types to include " << options["types-to-include"] << endl;
              std::ifstream includedTypesFile;
              includedTypesFile.open(options["types-to-include"], std::ifstream::in);

              if(includedTypesFile.is_open())
              {
                  std::string includedType;
                  while(std::getline(includedTypesFile, includedType))
                  {
                      cerr<<"Include Type: "<<includedType<<endl;
                      typesToInclude.insert(includedType);
                  }
              }
          }

          std::set<std::string> tablesToInclude;
          if (options.count("tables-to-include")) {
              cerr << "File containing tables to include " << options["tables-to-include"] << endl;
              std::ifstream includedTablesFile;
              includedTablesFile.open(options["tables-to-include"], std::ifstream::in);

              if(includedTablesFile.is_open())
              {
                  std::string includedTable;
                  while(std::getline(includedTablesFile, includedTable))
                  {
                      cerr<<"Include Table: "<<includedTable<<endl;
                      tablesToInclude.insert(includedTable);
                  }
              }
          }

          std::set<std::string> routinesToInclude;
          if (options.count("routines-to-include")) {
              cerr << "File containing routines to include " << options["routines-to-include"] << endl;
              std::ifstream includedRoutinesFile;
              includedRoutinesFile.open(options["routines-to-include"], std::ifstream::in);

              if(includedRoutinesFile.is_open())
              {
                  std::string includedRoutine;
                  while(std::getline(includedRoutinesFile, includedRoutine))
                  {
                      cerr<<"Include Routine: "<<includedRoutine<<endl;
                      routinesToInclude.insert(includedRoutine);
                  }
              }
          }
          
          std::set<std::string> aggregatesToInclude;
          if (options.count("aggregates-to-include")) {
              cerr << "File containing aggregates to include " << options["aggregates-to-include"] << endl;
              std::ifstream includedAggregatesFile;
              includedAggregatesFile.open(options["aggregates-to-include"], std::ifstream::in);

              if(includedAggregatesFile.is_open())
              {
                  std::string includedAggregate;
                  while(std::getline(includedAggregatesFile, includedAggregate))
                  {
                      cerr<<"Include Aggregate: "<<includedAggregate<<endl;
                      aggregatesToInclude.insert(includedAggregate);
                  }
              }
          }
		  
		  schema = make_shared<schema_pqxx>(options["target"], options.count("exclude-catalog"), typesToInclude, tablesToInclude, routinesToInclude, aggregatesToInclude);
      }

      scope scope;
      long queries_generated = 0;
      schema->fill_scope(scope);

      if (options.count("rng-state")) {
	   istringstream(options["rng-state"]) >> smith::rng;
      } else {
	   smith::rng.seed(options.count("seed") ? stoi(options["seed"]) : getpid());
      }

      vector<shared_ptr<logger> > loggers;

      loggers.push_back(make_shared<impedance_feedback>());

      if (options.count("log-to"))
	loggers.push_back(make_shared<pqxx_logger>(
	     options.count("sqlite") ? options["sqlite"] : options["target"],
	     options["log-to"], *schema));

      if (options.count("verbose")) {
	auto l = make_shared<cerr_logger>();
	global_cerr_logger = &*l;
	loggers.push_back(l);
	signal(SIGINT, cerr_log_handler);
      }
      
      if (options.count("dump-all-graphs"))
	loggers.push_back(make_shared<ast_logger>());

      if (options.count("dump-all-queries"))
	loggers.push_back(make_shared<query_dumper>());

      if (options.count("dry-run")) {
	while (1) {
	  shared_ptr<prod> gen = statement_factory(&scope);
	  gen->out(cout);
	  for (auto l : loggers)
	    l->generated(*gen);
	  cout << ";" << endl;
	  queries_generated++;

	  if (options.count("max-queries")
	      && (queries_generated >= stol(options["max-queries"])))
	      return 0;
	}
      }

      shared_ptr<dut_base> dut;
      
      if (options.count("sqlite")) {
#ifdef HAVE_LIBSQLITE3
	dut = make_shared<dut_sqlite>(options["sqlite"]);
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without SQLite support." << endl;
	return 1;
#endif
      }
      else if(options.count("monetdb")) {
#ifdef HAVE_MONETDB	   
	dut = make_shared<dut_monetdb>(options["monetdb"]);
#else
	cerr << "Sorry, " PACKAGE_NAME " was compiled without MonetDB support." << endl;
	return 1;
#endif
      }
      else
	dut = make_shared<dut_libpq>(options["target"]);

      while (1) /* Loop to recover connection loss */
      {
	try {
            while (1) { /* Main loop */

	    if (options.count("max-queries")
		&& (++queries_generated > stol(options["max-queries"]))) {
	      if (global_cerr_logger)
		global_cerr_logger->report();
	      return 0;
	    }
	    
	    /* Invoke top-level production to generate AST */
	    shared_ptr<prod> gen = statement_factory(&scope);

	    for (auto l : loggers)
	      l->generated(*gen);
	  
	    /* Generate SQL from AST */
	    ostringstream s;
	    gen->out(s);

	    /* Try to execute it */
	    try {
	      dut->test(s.str());
	      for (auto l : loggers)
		l->executed(*gen);
	    } catch (const dut::failure &e) {
	      for (auto l : loggers)
		try {
		  l->error(*gen, e);
		} catch (runtime_error &e) {
		  cerr << endl << "log failed: " << typeid(*l).name() << ": "
		       << e.what() << endl;
		}
	      if ((dynamic_cast<const dut::broken *>(&e))) {
		/* re-throw to outer loop to recover session. */
		throw;
	      }
	    }
	  }
	}
	catch (const dut::broken &e) {
	  /* Give server some time to recover. */
	  this_thread::sleep_for(milliseconds(1000));
	}
      }
    }
  catch (const exception &e) {
    cerr << e.what() << endl;
    return 1;
  }
}
