package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.LoadMTMVInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

public class LoadMTMVCommand extends Command implements ForwardWithSync, NotAllowFallback {
    private final LoadMTMVInfo loadMTMVInfo;

    public LoadMTMVCommand(LoadMTMVInfo loadMTMVInfo) {
        super(PlanType.LOAD_MTMV_COMMAND);
        this.loadMTMVInfo = Objects.requireNonNull(loadMTMVInfo, "require loadMTMVInfo object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        loadMTMVInfo.analyze(ctx);
        Env.getCurrentEnv().getMtmvService().loadMTMV(loadMTMVInfo);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLoadMTMVCommand(this, context);
    }
}
