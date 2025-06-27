export type EnvMode = 'browser' | 'node' | 'worker' | 'miniApp' | 'desktop-miniApp';
export type LooseFunction = (...args: any) => any;
export declare class Env {
    static _mode: EnvMode;
    static get mode(): EnvMode;
    static set mode(mode: EnvMode);
    static dpr: number;
    static CreateCanvas?: LooseFunction;
    static LoadImage?: LooseFunction;
    static RequestAnimationFrame?: LooseFunction;
    static CancelAnimationFrame?: LooseFunction;
    static RegisterCreateCanvas(func: LooseFunction): void;
    static RegisterLoadImage(func: LooseFunction): void;
    static GetCreateCanvasFunc(): LooseFunction | undefined;
    static RegisterRequestAnimationFrame(func: LooseFunction): void;
    static GetRequestAnimationFrame(): LooseFunction;
    static RegisterCancelAnimationFrame(func: LooseFunction): void;
    static GetCancelAnimationFrame(): LooseFunction;
}
