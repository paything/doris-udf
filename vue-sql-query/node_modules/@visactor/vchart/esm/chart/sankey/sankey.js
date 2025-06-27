import { BaseChart } from "../base/base-chart";

import { SeriesTypeEnum } from "../../series/interface/type";

import { registerSankeySeries } from "../../series/sankey/sankey";

import { Factory } from "../../core/factory";

import { SankeyChartSpecTransformer } from "./sankey-transformer";

import { isArray } from "@visactor/vutils";

import { loadScrollbar } from "@visactor/vrender-components";

export class SankeyChart extends BaseChart {
    constructor() {
        super(...arguments), this.transformerConstructor = SankeyChartSpecTransformer, this.type = "sankey", 
        this.seriesType = SeriesTypeEnum.sankey;
    }
    _setStateInDatum(stateKey, checkReverse, datum, filter, region) {
        const activeDatum = isArray(datum) ? datum[0] : datum;
        this.filterGraphicsByDatum(activeDatum, {
            filter: (series, mark) => "text" !== mark.type && mark.getProduct() && (!filter || filter(series, mark)),
            region: region,
            getDatum: e => {
                var _a;
                let d = null === (_a = e.getDatum()) || void 0 === _a ? void 0 : _a.datum;
                return isArray(d) && (d = d[0]), d;
            },
            callback: (element, mark, s, r) => {
                var _a, _b, _c;
                const id = null === (_a = mark.getProduct()) || void 0 === _a ? void 0 : _a.id();
                id && (id.includes("node") || id.includes("link")) && (null === (_c = (_b = s)._handleEmphasisElement) || void 0 === _c || _c.call(_b, {
                    item: element
                }));
            },
            regionCallback: (elements, r) => {
                activeDatum ? elements.length && (elements.forEach((e => {
                    r.interaction.startInteraction(stateKey, e);
                })), checkReverse && r.interaction.reverseEventElement(stateKey)) : r.interaction.clearEventElement(stateKey, !0);
            }
        });
    }
}

SankeyChart.type = "sankey", SankeyChart.seriesType = SeriesTypeEnum.sankey, SankeyChart.transformerConstructor = SankeyChartSpecTransformer;

export const registerSankeyChart = () => {
    loadScrollbar(), registerSankeySeries(), Factory.registerChart(SankeyChart.type, SankeyChart);
};
//# sourceMappingURL=sankey.js.map
