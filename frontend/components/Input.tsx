"use client";

export const Input = ({label, placeholder, onChange, type = "text"}: {
    label: string;
    placeholder: string;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    onChange: (e: any) => void;
    type?: "text" | "password"
}) => {
    return <div>
        <div className="text-sm pb-1 pt-2">
            * <label>{label}</label>
        </div>
        <input className="border rounded px-4 py-2 w-full border-black" type={type} placeholder={placeholder} onChange={onChange} />
    </div>
}