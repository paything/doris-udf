"use strict";

Object.defineProperty(exports, "__esModule", {
    value: !0
}), exports.registerSankeyChart = exports.SankeyChart = void 0;

const base_chart_1 = require("../base/base-chart"), type_1 = require("../../series/interface/type"), sankey_1 = require("../../series/sankey/sankey"), factory_1 = require("../../core/factory"), sankey_transformer_1 = require("./sankey-transformer"), vutils_1 = require("@visactor/vutils"), vrender_components_1 = require("@visactor/vrender-components");

class SankeyChart extends base_chart_1.BaseChart {
    constructor() {
        super(...arguments), this.transformerConstructor = sankey_transformer_1.SankeyChartSpecTransformer, 
        this.type = "sankey", this.seriesType = type_1.SeriesTypeEnum.sankey;
    }
    _setStateInDatum(stateKey, checkReverse, datum, filter, region) {
        const activeDatum = (0, vutils_1.isArray)(datum) ? datum[0] : datum;
        this.filterGraphicsByDatum(activeDatum, {
            filter: (series, mark) => "text" !== mark.type && mark.getProduct() && (!filter || filter(series, mark)),
            region: region,
            getDatum: e => {
                var _a;
                let d = null === (_a = e.getDatum()) || void 0 === _a ? void 0 : _a.datum;
                return (0, vutils_1.isArray)(d) && (d = d[0]), d;
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

exports.SankeyChart = SankeyChart, SankeyChart.type = "sankey", SankeyChart.seriesType = type_1.SeriesTypeEnum.sankey, 
SankeyChart.transformerConstructor = sankey_transformer_1.SankeyChartSpecTransformer;

const registerSankeyChart = () => {
    (0, vrender_components_1.loadScrollbar)(), (0, sankey_1.registerSankeySeries)(), 
    factory_1.Factory.registerChart(SankeyChart.type, SankeyChart);
};

exports.registerSankeyChart = registerSankeyChart;
//# sourceMappingURL=sankey.js.map
