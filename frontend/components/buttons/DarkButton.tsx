import { ReactNode } from "react"

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const DarkButton = ({ children, onClick, size = "small" }: {
    children: ReactNode,
    onClick: () => void,
    size?: "big" | "small"
}) => {
    return <div onClick={onClick} className={`flex flex-col justify-center px-8 py-2 cursor-pointer hover:shadow-md bg-purple-800 text-white rounded text-center`}>
        {children}
    </div>
}