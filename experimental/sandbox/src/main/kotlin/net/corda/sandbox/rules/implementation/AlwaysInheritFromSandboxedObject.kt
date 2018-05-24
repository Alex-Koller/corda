package net.corda.sandbox.rules.implementation

import net.corda.sandbox.analysis.AnalysisRuntimeContext
import net.corda.sandbox.code.ClassDefinitionProvider
import net.corda.sandbox.code.Emitter
import net.corda.sandbox.code.EmitterContext
import net.corda.sandbox.code.Instruction
import net.corda.sandbox.code.instructions.MemberAccessInstruction
import net.corda.sandbox.code.instructions.TypeInstruction
import net.corda.sandbox.references.Class
import org.objectweb.asm.Opcodes
import java.lang.reflect.Modifier

/**
 * Definition provider that ensures that all objects inherit from a sandboxed version of [java.lang.Object], with a
 * deterministic `hashCode()` method.
 */
@Suppress("unused")
class AlwaysInheritFromSandboxedObject : ClassDefinitionProvider, Emitter {

    override fun define(context: AnalysisRuntimeContext, clazz: Class) = when {
        isDirectSubClassOfObject(context.clazz) -> clazz.copy(superClass = SANDBOX_OBJECT_NAME)
        else -> clazz
    }

    override fun emit(context: EmitterContext, instruction: Instruction) = context.emit {
        if (instruction is TypeInstruction &&
                instruction.typeName == OBJECT_NAME) {
            // When creating new objects, make sure the sandboxed type gets used.
            new(SANDBOX_OBJECT_NAME, instruction.operation)
            preventDefault()
        }
        if (instruction is MemberAccessInstruction &&
                instruction.operation == Opcodes.INVOKESPECIAL &&
                instruction.owner == OBJECT_NAME &&
                instruction.memberName == CONSTRUCTOR_NAME &&
                context.clazz.name != SANDBOX_OBJECT_NAME) {
            // Rewrite object initialisation call so that the sandboxed constructor gets used instead.
            loadConstant(0xfed_c0de)
            invokeSpecial(SANDBOX_OBJECT_NAME, CONSTRUCTOR_NAME, "(I)V", instruction.ownerIsInterface)
            preventDefault()
        }
    }

    private fun isDirectSubClassOfObject(clazz: Class): Boolean {
        // Check if the super class is java.lang.Object and that current class is not sandbox.java.lang.Object.
        val isClass = !Modifier.isInterface(clazz.access)
        return isClass && isObject(clazz.superClass) && clazz.name != SANDBOX_OBJECT_NAME
    }

    private fun isObject(superClass: String) = superClass.isBlank() || superClass == OBJECT_NAME

    companion object {

        private const val OBJECT_NAME = "java/lang/Object"

        private const val SANDBOX_OBJECT_NAME = "sandbox/java/lang/Object"

        private const val CONSTRUCTOR_NAME = "<init>"

    }

}
