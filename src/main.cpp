#include "duckdb.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/parser.hpp"

#include "gpos/_api.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/init.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CLogicalConstTableGet.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarProjectElement.h"
#include "gpopt/operators/CScalarProjectList.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/metadata/CName.h"
#include "naucrates/init.h"
#include "naucrates/base/IDatumInt4.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/CMDProviderGeneric.h"
#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/md/CSystemId.h"
#include "naucrates/md/IMDTypeInt4.h"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

class GporcaRuntime {
public:
  GporcaRuntime() {
    gpos_init_params params = {nullptr};
    gpos_init(&params);
    gpdxl_init();
    InitDXL();
    gpopt_init();
  }

  ~GporcaRuntime() {
    gpopt_terminate();
    ShutdownDXL();
    gpdxl_terminate();
    gpos_terminate();
  }
};

struct ConvertedQuery {
  gpopt::CExpression *expr = nullptr;
  gpos::ULongPtrArray *output_col_ids = nullptr;
  gpmd::CMDNameArray *output_col_names = nullptr;
};

ConvertedQuery BuildGporcaLogicalTreeFromSql(bool needs_projection,
                                                gpos::CMemoryPool *mp,
                                                gpopt::CMDAccessor *md_accessor,
                                                const gpmd::CSystemId &sysid) {
  ConvertedQuery converted;

  auto *opt_ctxt = gpopt::COptCtxt::PoctxtFromTLS();
  auto *col_factory = opt_ctxt->Pcf();
  const auto *int4_type = md_accessor->PtMDType<gpmd::IMDTypeInt4>(sysid);

  auto *input_cols = GPOS_NEW(mp) gpopt::CColRefArray(mp);
  gpos::CWStringConst input_col_name_wstr(GPOS_WSZ_LIT("duckdb_input_col"));
  gpopt::CName input_col_name(mp, &input_col_name_wstr);
  gpopt::CColRef *input_col =
      col_factory->PcrCreate(int4_type, gpmd::default_type_modifier, input_col_name);
  input_cols->Append(input_col);

  auto *input_row = GPOS_NEW(mp) gpnaucrates::IDatumArray(mp);
  input_row->Append(static_cast<gpnaucrates::IDatum *>(int4_type->CreateInt4Datum(mp, 42, false)));
  auto *rows = GPOS_NEW(mp) gpopt::IDatum2dArray(mp);
  rows->Append(input_row);

  gpopt::CExpression *current = GPOS_NEW(mp)
      gpopt::CExpression(mp, GPOS_NEW(mp) gpopt::CLogicalConstTableGet(mp, input_cols, rows));

  gpopt::CColRef *output_col = input_col;
  if (needs_projection) {
    gpos::CWStringConst output_col_name_wstr(GPOS_WSZ_LIT("answer"));
    gpopt::CName output_col_name(mp, &output_col_name_wstr);
    output_col = col_factory->PcrCreate(int4_type, gpmd::default_type_modifier, output_col_name);

    auto *project_expr =
        GPOS_NEW(mp) gpopt::CExpression(mp, GPOS_NEW(mp) gpopt::CScalarIdent(mp, input_col));
    auto *project_element = GPOS_NEW(mp) gpopt::CExpression(
        mp, GPOS_NEW(mp) gpopt::CScalarProjectElement(mp, output_col), project_expr);
    auto *project_list =
        GPOS_NEW(mp) gpopt::CExpression(mp, GPOS_NEW(mp) gpopt::CScalarProjectList(mp), project_element);

    current = GPOS_NEW(mp)
        gpopt::CExpression(mp, GPOS_NEW(mp) gpopt::CLogicalProject(mp), current, project_list);
  }

  converted.expr = current;
  converted.output_col_ids = GPOS_NEW(mp) gpos::ULongPtrArray(mp);
  converted.output_col_ids->Append(GPOS_NEW(mp) gpos::ULONG(output_col->Id()));
  converted.output_col_names = GPOS_NEW(mp) gpmd::CMDNameArray(mp);
  converted.output_col_names->Append(GPOS_NEW(mp) gpmd::CMDName(mp, output_col->Name().Pstr()));
  return converted;
}

void PrintGporcaTree(const gpopt::CExpression *expr, int indent) {
  if (!expr) {
    return;
  }
  for (int i = 0; i < indent; i++) {
    std::cout << ' ';
  }
  std::cout << expr->Pop()->SzId() << std::endl;
  for (gpos::ULONG i = 0; i < expr->Arity(); i++) {
    PrintGporcaTree((*expr)[i], indent + 2);
  }
}

std::string ResolveMetadataPath() {
  if (const char *env_path = std::getenv("GPORCA_MD_FILE")) {
    return std::string(env_path);
  }
  const std::filesystem::path src_file(__FILE__);
  const auto repo_root = src_file.parent_path().parent_path();
  return (repo_root / "third_party/gporca/data/dxl/metadata/md.xml").string();
}

std::string MapGporcaPhysicalToDuckdb(const char *gporca_op_name) {
  const std::string op = gporca_op_name ? gporca_op_name : "";
  if (op == "CPhysicalComputeScalar") {
    return "PROJECTION";
  }
  if (op == "CPhysicalConstTableGet") {
    return "DUMMY_SCAN";
  }
  return op;
}

void BuildDuckdbPlanFromGporcaPhysical(const gpopt::CExpression *expr, int indent,
                                       std::ostringstream &os) {
  if (!expr) {
    return;
  }
  const std::string mapped = MapGporcaPhysicalToDuckdb(expr->Pop()->SzId());
  for (int i = 0; i < indent; i++) {
    os << ' ';
  }
  os << mapped << "\n";
  for (gpos::ULONG i = 0; i < expr->Arity(); i++) {
    BuildDuckdbPlanFromGporcaPhysical((*expr)[i], indent + 2, os);
  }
}

struct OptimizePayload {
  bool needs_projection = true;
  const std::string *metadata_path = nullptr;
  bool optimized = false;
  std::string duckdb_physical_plan;
};

void *OptimizeConvertedPlan(void *arg) {
  auto *payload = static_cast<OptimizePayload *>(arg);
  gpos::CAutoMemoryPool amp;
  auto *mp = amp.Pmp();

  gpopt::CMDCache::Reset();
  gpopt::CMDCache::Init();

  auto *pmdp = GPOS_NEW(mp) gpmd::CMDProviderMemory(mp, payload->metadata_path->c_str());
  gpmd::CSystemId sysid = gpmd::CMDProviderGeneric::SysidDefault();

  gpopt::CMDAccessor md_accessor(mp, gpopt::CMDCache::Pcache(), sysid, pmdp);
  gpopt::CAutoOptCtxt auto_opt_ctxt(mp, &md_accessor, nullptr,
                                    static_cast<gpopt::COptimizerConfig *>(nullptr));

  auto converted = BuildGporcaLogicalTreeFromSql(payload->needs_projection, mp, &md_accessor, sysid);
  if (!converted.expr || !converted.output_col_ids || !converted.output_col_names) {
    return nullptr;
  }

  std::cout << "Converted GPORCA logical tree:" << std::endl;
  PrintGporcaTree(converted.expr, 0);

  auto *query_context = gpopt::CQueryContext::PqcGenerate(
      mp, converted.expr, converted.output_col_ids, converted.output_col_names, true /*fDeriveStats*/);
  converted.output_col_ids->Release();
  converted.output_col_names->Release();

  gpopt::CEngine engine(mp);
  engine.Init(query_context, nullptr /*search stage array*/);
  engine.Optimize();

  auto *optimized_plan = engine.PexprExtractPlan();
  if (optimized_plan) {
    std::cout << "Optimized GPORCA plan tree:" << std::endl;
    PrintGporcaTree(optimized_plan, 0);

    std::ostringstream duckdb_plan_os;
    BuildDuckdbPlanFromGporcaPhysical(optimized_plan, 0, duckdb_plan_os);
    payload->duckdb_physical_plan = duckdb_plan_os.str();

    optimized_plan->Release();
    payload->optimized = true;
  }

  GPOS_DELETE(query_context);
  return nullptr;
}

bool ConvertDuckdbPlanFromGporcaPhysicalTree(bool needs_projection) {
  GporcaRuntime runtime;

  const auto md_path = ResolveMetadataPath();
  if (!std::filesystem::exists(md_path)) {
    std::cout << "Metadata file not found: " << md_path << std::endl;
    return false;
  }

  OptimizePayload payload;
  payload.needs_projection = needs_projection;
  payload.metadata_path = &md_path;

  char error_buffer[4096] = {0};
  gpos_exec_params exec_params;
  exec_params.func = OptimizeConvertedPlan;
  exec_params.arg = &payload;
  exec_params.stack_start = &exec_params;
  exec_params.error_buffer = error_buffer;
  exec_params.error_buffer_size = sizeof(error_buffer);
  exec_params.abort_requested = nullptr;

  const int exec_status = gpos_exec(&exec_params);
  if (gpopt::CMDCache::FInitialized()) {
    gpopt::CMDCache::Shutdown();
  }

  if (exec_status != 0) {
    std::cout << "GPORCA optimization failed." << std::endl;
    if (error_buffer[0] != '\0') {
      std::cout << "GPORCA error: " << error_buffer << std::endl;
    }
    return false;
  }

  if (!payload.optimized) {
    std::cout << "GPORCA did not produce an optimized plan." << std::endl;
    return false;
  }

  std::cout << "DuckDB physical plan (converted from GPORCA physical tree):" << std::endl;
  std::cout << payload.duckdb_physical_plan;

  return true;
}

} // namespace

int main() {
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);

  // Edit this SQL string to change the statement executed by this program.
  const std::string sql = "SELECT 42 AS answer, 'hello from sql_reader' AS note";

  // Parse SQL text into DuckDB AST statements.
  duckdb::Parser parser;
  try {
    parser.ParseQuery(sql);
  } catch (const std::exception &ex) {
    std::cerr << "SQL parse failed: " << ex.what() << std::endl;
    return 1;
  }

  std::cout << "Parsed statements: " << parser.statements.size() << std::endl;
  if (!parser.statements.empty()) {
    std::cout << "AST:\n" << parser.statements[0]->ToString() << std::endl;
  }

  std::cout << "GPORCA direct conversion:" << std::endl;
  // For this demo query shape, SQL SELECT maps to a projection over a constant scan.
  const bool needs_projection = true;
  ConvertDuckdbPlanFromGporcaPhysicalTree(needs_projection);
  
  auto result = con.Query(sql);
  if (result->HasError()) {
    std::cerr << "SQL execution failed: " << result->GetError() << std::endl;
    return 1;
  }

  result->Print();
  return 0;
}
